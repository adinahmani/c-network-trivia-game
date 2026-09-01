//
// Created by BasharJabaly on 6/25/2025.
//
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

/* ─────────── Configuration ─────────────────────────────────────────── */
#define QSVR_PORT          1236
#define PKT_BUF            4096
#define Q_MCAST_GRP        "239.0.0.25"
#define Q_MCAST_PORT       1212
#define STR_MAX            256
#define CHOICE_MAX         100
#define PEERS_MAX          100
#define PING_INTERVAL      10      /* sec */

/* ─────────── Socket end‑points ─────────────────────────────────────── */
static struct sockaddr_in g_srv, g_mcBind;
static int  g_mcPort      = 0;
static char g_mcGroup[64] = {0};
static int  g_joinPhase   = 0;     /* 0 = expect group, 1 = expect port */
static char g_myNick[32]  = {0};

/* ─────────── Message types (keep in sync with server) ─────────────── */
typedef enum {
    MSG_MCAST_TEXT,
    MSG_QUESTION,
    MSG_ACK,
    MSG_UCAST_TEXT,
    MSG_SCORE_TABLE,
    MSG_KEEP_ALIVE,
    MSG_MCAST_PARAM,
    MSG_GAME_OVER
} MsgKind;

/* composite payloads (same layout client ↔ server) */

typedef struct {
    MsgKind kind;
    char    text[STR_MAX];
} TextPkt;

typedef struct {
    MsgKind kind;
    int     number;
    char    qtext[STR_MAX];
    char    choices[4][CHOICE_MAX];
    int     right_choice;
} QuizPkt;

typedef struct {
    MsgKind kind;
    int     ok; /* 1 */
} AckPkt;

typedef struct {
    MsgKind kind;
    int     nPeers;
    char    names[PEERS_MAX][32];
    int     ids[PEERS_MAX];
    int     scores[PEERS_MAX];
} ScorePkt;

/* ─────────── Helper: pretty‑print a quiz ───────────────────────────── */
static void quiz_print(const QuizPkt *q)
{
    printf("%d: %s\n\n", q->number, q->qtext);
    printf("    A: %s\n", q->choices[0]);
    printf("    B: %s\n", q->choices[1]);
    printf("    C: %s\n", q->choices[2]);
    printf("    D: %s\n", q->choices[3]);
    printf("Your answer: ");
    fflush(stdout);
}

/* ─────────── Thread: handle stdin → server --------------------------- */
static void *thr_stdin(void *arg)
{
    int fd = *(int *)arg;
    char line[STR_MAX];
    TextPkt pkt = { .kind = MSG_UCAST_TEXT };

    while (fgets(line, sizeof line, stdin)) {
        line[strcspn(line, "\n")] = '\0';
        strncpy(pkt.text, line, sizeof pkt.text - 1);
        send(fd, &pkt, sizeof pkt, 0);
    }
    return NULL;
}

/* ─────────── Thread: TCP (unicast) listener -------------------------- */
static void *thr_tcp_rx(void *arg)
{
    int fd = *(int *)arg;
    char buf[PKT_BUF];

    while (1) {
        MsgKind k;
        if (recv(fd, &k, sizeof k, MSG_PEEK) <= 0) break;

        switch (k) {
            case MSG_UCAST_TEXT: {
                TextPkt p; recv(fd, &p, sizeof p, 0);
                if (!strncmp(p.text, "NAME_OK:", 8)) {
                    strncpy(g_myNick, p.text + 8, sizeof g_myNick - 1);
                    printf("Nickname confirmed: %s\n", g_myNick);
                } else {
                    printf("%s\n", p.text);
                }
                break; }

            case MSG_KEEP_ALIVE: {
                AckPkt a; recv(fd, &a, sizeof a, 0);
                //printf("[CLI] keep-alive ping received; sending ACK\n");
                send(fd, &a, sizeof a, 0);
                break; }

            case MSG_MCAST_PARAM: {
                TextPkt p; recv(fd, &p, sizeof p, 0);
                if (g_joinPhase == 0) {
                    strncpy(g_mcGroup, p.text, sizeof g_mcGroup - 1);
                    g_joinPhase = 1;
                } else {
                    g_mcPort = atoi(p.text);
                    g_joinPhase = 2;
                }
                break; }

            default: /* drain unknown */
                recv(fd, buf, sizeof k, 0);
        }
    }
    return NULL;
}

/* ─────────── Thread: UDP multicast listener -------------------------- */
static void *thr_mc_rx(void *arg)
{
    int *fds = arg;
    int mcfd = fds[1];
    int i;
    char buf[PKT_BUF];
    socklen_t alen = sizeof(struct sockaddr_in);

    while (1) {
        ssize_t n = recvfrom(mcfd, buf, sizeof buf, 0,
                             (struct sockaddr *)&g_mcBind, &alen);
        if (n < (ssize_t)sizeof(MsgKind)) continue;
        MsgKind k; memcpy(&k, buf, sizeof k);

        switch (k) {
            case MSG_MCAST_TEXT: {
                TextPkt p; memcpy(&p, buf, sizeof p);
                printf("[MC] %s\n", p.text);
                break; }

            case MSG_QUESTION: {
                QuizPkt q; memcpy(&q, buf, sizeof q);
                quiz_print(&q);
                break; }

            case MSG_SCORE_TABLE: {
                ScorePkt s; memcpy(&s, buf, sizeof s);
                puts("\n--- SCOREBOARD ---");
                for ( i = 0; i < s.nPeers; i++)
                    printf("%-12s : %d\n", s.names[i], s.scores[i]);
                puts("-------------------");
                break; }
            case MSG_GAME_OVER:              /* ← new */
                printf("[MC] Game over — back to lobby.\n");
            goto done;

            default:
                fprintf(stderr, "Unknown multicast pkt %d\n", k);
        }
    }
    done:
    return NULL;
}

/* ─────────── main ---------------------------------------------------- */
/* ─────────── main ---------------------------------------------------- */
int main(void)
{
    /* 1) connect to quiz server */
    int ufd = socket(AF_INET, SOCK_STREAM, 0);
    g_srv.sin_family = AF_INET;
    g_srv.sin_port   = htons(QSVR_PORT);
    g_srv.sin_addr.s_addr = inet_addr("192.168.5.5");
    if (connect(ufd, (struct sockaddr*)&g_srv, sizeof g_srv) < 0) {
        perror("connect");
        return 1;
    }
    puts("Connected to server.");

    /* 2) launch stdin + TCP‑rx threads */
    pthread_t tStdin, tTcpRx, tMcRx;
    pthread_create(&tStdin, NULL, thr_stdin, &ufd);
    pthread_detach(tStdin);
    pthread_create(&tTcpRx, NULL, thr_tcp_rx, &ufd);
    pthread_detach(tTcpRx);

    /* 3) wait until server sent group + port */
    for (;;) {
        g_joinPhase = 0;
        while (g_joinPhase < 2) sleep(1);

        /* 4) set up multicast socket */
        int mcfd = socket(AF_INET, SOCK_DGRAM, 0);
        int reuse = 1;
        setsockopt(mcfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);

        g_mcBind.sin_family      = AF_INET;
        g_mcBind.sin_port        = htons(g_mcPort);
        g_mcBind.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(mcfd, (struct sockaddr*)&g_mcBind, sizeof g_mcBind) < 0) {
            perror("bind mcast");
            return 1;
        }

        struct ip_mreq mreq = {
            .imr_multiaddr.s_addr = inet_addr(g_mcGroup),
            .imr_interface.s_addr = htonl(INADDR_ANY)
        };
        if (setsockopt(mcfd, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                       &mreq, sizeof mreq) < 0) {
            perror("join group");
            return 1;
                       }

        /* 5) start multicast listener */
        int fdPair[2] = { ufd, mcfd };
        pthread_create(&tMcRx, NULL, thr_mc_rx, fdPair);
        pthread_join(tMcRx, NULL);
        struct ip_mreq drop = {
            .imr_multiaddr.s_addr = inet_addr(g_mcGroup),
            .imr_interface.s_addr = htonl(INADDR_ANY)
        };
        setsockopt(mcfd, IPPROTO_IP, IP_DROP_MEMBERSHIP,
                   &drop, sizeof drop);
        close(mcfd);
    }
    return 0;
}
