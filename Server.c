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
#include <signal.h>
#include <stdbool.h>
#include <time.h>
/* ─────────────  Compile-time settings  ─────────────────────────────── */
#define QUIZ_TCP_PORT     1236
#define QUIZ_MC_GRP      "239.0.0.25"
#define QUIZ_MC_PORT      1212
#define TCP_BACKLOG          3
#define MAX_PEERS          100
#define MAX_BUFFER        4096
#define MAX_LINES          256
#define MAX_CHOICES        100
#define MAX_ROUNDS          10
#define PING_GAP            10         /* sec */
#define TIMEOUT_SECS        10
#define INET_ADDRSTRLEN  16
#define AUTH_CODE     "ABCDEFG"
#define STRIFY(x) #x
#define TOSTRING(x) STRIFY(x)
/* ─────────────  Datagram types (must match client)  ────────────────── */
typedef enum {
    PKT_MC_TEXT,
    PKT_QUESTION,
    PKT_ACK,
    PKT_UC_TEXT,
    PKT_SCOREBRD,
    PKT_PING,
    PKT_MC_PARAM,
    MSG_GAME_OVER
} PktType;
/* ─────────────  Wire structures  ───────────────────────────────────── */
typedef struct {
    PktType kind;
    int qNo;
    char question[MAX_LINES];
    char choice[4][MAX_CHOICES];
    int right;
} QuizQuestion;
typedef struct {
    PktType kind;
    int ok;
} PingAck;
typedef struct {
    PktType kind;
    char msg[MAX_LINES];
} TextMsg;
typedef struct {
    PktType kind;
    int nPeers;
    char name[MAX_PEERS][32];
    int id[MAX_PEERS];
    int pts[MAX_PEERS];
} ScoreBoard;
/* ─────────────  Per-player bookkeeping  ────────────────────────────── */
typedef struct {
    int fd;
    int id;
    int answer; /* 1-4 or −1 */
    int score;
    time_t lastSeen;
    char nick[32];
    bool inLobby;
    bool ready;
    bool inGame;
    bool freed;
    char ip[INET_ADDRSTRLEN];
} Player;
/* ─────────────  Globals  ───────────────────────────────────────────── */
static int g_sockTCP = -1;
static int g_sockMC = -1;
static struct sockaddr_in g_mcAddr;
static int g_clientFDs[MAX_PEERS];
static Player *g_players[MAX_PEERS];
static int g_peerCnt = 0;
static int g_readyPeers = 0;
static int g_nextID = 0;
static bool g_runningGame = false;
static QuizQuestion g_questions[MAX_ROUNDS];
/* mutexes */
static pthread_mutex_t mxPeers = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mxBuffers = PTHREAD_MUTEX_INITIALIZER;
//--------------functions prototpe-----------------
/* ── low-level I/O helpers ───────────────────────────── */
static void mc_send(const void *buf, size_t len);
static void uc_send(int fd, const void *buf, size_t len);
/* ── question preparation ────────────────────────────── */
static int load_questions(const char *filename);
static void shuffle_questions(int n);
/* ── player-count / lobby utilities ──────────────────── */
static int live_ingame(void); /* # active contestants       */
static Player *sole_survivor(void); /* ptr to last player         */
static void reset_lobby(void); /* move everyone to lobby     */
static void drop_player(Player *pl); /* erase & free a player slot */
/* ── per-player threads ──────────────────────────────── */
static void *thr_keepalive(void *arg);
static void *thr_player_rx(void *arg);
/* ── listener + game coordinator ─────────────────────── */
static void *thr_accept(void *arg);
static void run_game(void);
//--------------functions prototpe-----------------
static void drop_player(Player *pl) {
    int i;
    /* remember if this player was “in the game” (pressed R) */
    bool wasSeated = !pl->inLobby;
    if (!pl) return;
    pthread_mutex_lock(&mxPeers);
    if (pl->freed) {
        /* already done */
        pthread_mutex_unlock(&mxPeers);
        return;
    }
    pl->freed = true; /* mark first   */
    pthread_mutex_unlock(&mxPeers);
    close(pl->fd);
    pl->fd = -1;
    /* remove from array */
    pthread_mutex_lock(&mxPeers);
    for (i = 0; i < g_peerCnt; i++)
        if (g_players[i] == pl) {
            g_players[i] = g_players[g_peerCnt - 1];
            g_players[g_peerCnt - 1] = NULL;
            g_peerCnt--;
            break;
        }
    /* if they were counted as “ready,” un-count them */
    if (wasSeated && g_readyPeers > 0) {
        g_readyPeers--;
        printf("[SVR] player left, ready count now %d\n", g_readyPeers);
    }
    pthread_mutex_unlock(&mxPeers);
    free(pl); /* freed once   */
}
static void reset_lobby(void) {
    int i;
    pthread_mutex_lock(&mxPeers);
    g_readyPeers = 0;
    for (i = 0; i < g_peerCnt; i++) {
        if (g_players[i] && g_players[i]->fd != -1) {
            g_players[i]->inLobby = true;
            g_players[i]->answer = -1;
            g_players[i]->ready = false;
            g_players[i]->inGame = false;
            g_players[i]->score = 0; /* NEW: reset points       */
        }
    }
    pthread_mutex_unlock(&mxPeers);
    /* inform all survivors */
    TextMsg msg = {.kind = PKT_UC_TEXT};
    strcpy(msg.msg, "Match over! Press 'R' to join the next game.");
    for (i = 0; i < g_peerCnt; i++)
        if (g_players[i] && g_players[i]->fd != -1)
            uc_send(g_players[i]->fd, &msg, sizeof msg);
    /* multicast to any new spectators */
    TextMsg info = {.kind = PKT_MC_TEXT};
    strcpy(info.msg, "Lobby open for the next game.");
    mc_send(&info, sizeof info);
}
/* ───── helper: count live players who are IN the current match ───── */
static int live_ingame(void) {
    int n = 0, i;
    for (i = 0; i < g_peerCnt; i++)
        if (g_players[i]
            && g_players[i]->fd != -1 /* socket still open        */
            && !g_players[i]->inLobby) /* already seated in match  */
            n++;
    return n;
}
/* ───── helper: return pointer to the last player standing ───── */
static Player *sole_survivor(void) {
    int i;
    for (i = 0; i < g_peerCnt; i++)
        if (g_players[i]
            && g_players[i]->fd != -1
            && !g_players[i]->inLobby)
            return g_players[i];
    return NULL;
}
/* ─────────────  Helper: multicast send  ────────────────────────────── */
static void mc_send(const void *buf, size_t len) {
    if (sendto(g_sockMC, buf, len, 0,
               (struct sockaddr *) &g_mcAddr,
               sizeof g_mcAddr) < 0)
        perror("sendto-mc");
}
/* ─────────────  Helper: unicast send  ──────────────────────────────── */
static void uc_send(int fd, const void *buf, size_t len) {
    send(fd, buf, len, 0);
}
/* ─────────────  Read questions from file  ──────────────────────────── */
static int load_questions(const char *file) {
    FILE *fp = fopen(file, "r");
    int c;
    if (!fp) {
        perror(file);
        return 0;
    }
    char line[MAX_LINES];
    int idx = 0;
    while (idx < MAX_ROUNDS && fgets(line, sizeof line, fp)) {
        QuizQuestion *q = &g_questions[idx];
        q->kind = PKT_QUESTION;
        char *tok = strtok(line, ";");
        if (!tok) continue;
        q->qNo = atoi(tok);
        tok = strtok(NULL, ";");
        strncpy(q->question, tok ? tok : "", MAX_LINES);
        for (c = 0; c < 4; c++) {
            tok = strtok(NULL, ";");
            strncpy(q->choice[c], tok ? tok : "", MAX_CHOICES);
        }
        tok = strtok(NULL, ";");
        q->right = tok ? atoi(tok) : 1;
        idx++;
    }
    fclose(fp);
    return idx;
}
/* ─────────────  Shuffle question order  ────────────────────────────── */
static void shuffle_questions(int n) {
    int i;
    for (i = 0; i < n - 1; i++) {
        int j = i + rand() / (RAND_MAX / (n - i) + 1);
        QuizQuestion tmp = g_questions[i];
        g_questions[i] = g_questions[j];
        g_questions[j] = tmp;
    }
}
/* ─────────────  Keep-alive thread per player  ──────────────────────── */
static void *thr_keepalive(void *arg) {
    Player *p = arg;
    PingAck ping = {.kind = PKT_PING, .ok = 1};
    while (1) {
        sleep(PING_GAP);
        uc_send(p->fd, &ping, sizeof ping);
        sleep(PING_GAP);
        if (time(NULL) - p->lastSeen > TIMEOUT_SECS) {
            printf("Player %d timed-out\n", p->id);
            close(p->fd); /* NEW: flag as disconnected   */
            if (!g_runningGame) {
                /* lobby: adjust ready count   */
                pthread_mutex_lock(&mxPeers);
                if (g_readyPeers > 0) g_readyPeers--;
                pthread_mutex_unlock(&mxPeers);
            }
            /* during a running game we don’t need extra work:
               run_game() will ignore any player whose fd == -1
               */
            drop_player(p);
            pthread_exit(NULL);
        }
    }
}
/* ─────────────  Player RX handler  ─────────────────────────────────── */
static void *thr_player_rx(void *arg) {
    Player *pl = arg;
    TextMsg txt;
    PingAck ack;
    PktType kind;
    int i;
    time_t authStart = time(NULL);
    /* greet */
    txt.kind = PKT_UC_TEXT;
    strcpy(txt.msg, "Welcome! Enter serial code (you have 1 minute):\n");
    uc_send(pl->fd, &txt, sizeof txt);
    int phase = 0; /* 0 = expect key, 1 = nickname */
    while (1) {
        // auth timeout
        if (phase < 2 && time(NULL) - authStart >= 60) {
            TextMsg tooLate = { .kind = PKT_UC_TEXT };
            strcpy(tooLate.msg, "Authentication timeout. Bye.\n");
            uc_send(pl->fd, &tooLate, sizeof tooLate);
            drop_player(pl);
            return NULL;
        }
        if (recv(pl->fd, &kind, sizeof kind, MSG_PEEK) <= 0) break;
        switch (kind) {
            case PKT_UC_TEXT:
                recv(pl->fd, &txt, sizeof txt, 0);
                if (phase == 0) {
                    /* auth */
                    if (strcmp(txt.msg, AUTH_CODE) == 0) {
                        strcpy(txt.msg, "Auth OK, enter nickname:");
                        uc_send(pl->fd, &txt, sizeof txt);
                        phase = 1;
                    } else {
                        strcpy(txt.msg, "Bad code, try again:");
                        uc_send(pl->fd, &txt, sizeof txt);
                    }
                } else if (phase == 1) {
                    /* nickname */
                    pthread_mutex_lock(&mxPeers);
                    bool taken = false;
                    for (i = 0; i < g_peerCnt; i++)
                        if (g_players[i] && strcmp(g_players[i]->nick, txt.msg) == 0)
                            taken = true;
                    pthread_mutex_unlock(&mxPeers);
                    if (taken) {
                        strcpy(txt.msg, "Name taken, try another:");
                        uc_send(pl->fd, &txt, sizeof txt);
                    } else {
                        strncpy(pl->nick, txt.msg, sizeof pl->nick - 1);
                        TextMsg ok = {.kind = PKT_UC_TEXT};
                        snprintf(ok.msg, sizeof ok.msg, "NAME_OK:%s", pl->nick);
                        uc_send(pl->fd, &ok, sizeof ok);
                        if (g_runningGame) {
                            strcpy(txt.msg,
                                   "Game already in progress; you’re in the lobby for the next round.");
                            pl->inLobby = true;
                            uc_send(pl->fd, &txt, sizeof txt);
                            /* do NOT set readyPeers here */
                        } else {
                            strcpy(txt.msg, "Press 'R' to sit in the lobby.");
                            uc_send(pl->fd, &txt, sizeof txt);
                        }
                        phase = 2;
                    }
                } else {
                    /* in lobby / game */
                    char c = txt.msg[0];
                    if ((c == 'R' || c == 'r') && g_runningGame) {
                        TextMsg game_started = {.kind = PKT_UC_TEXT};
                        strcpy(game_started.msg,
                  "A new game has begun!  Wait for it to end.\n");
                        uc_send(pl->fd, &game_started, sizeof game_started);
                    }
                    else if ((c == 'R' || c == 'r') && pl->inLobby) {
                        pl->inLobby = false;
                        pl->ready   = true;
                        pthread_mutex_lock(&mxPeers);
                        g_readyPeers++;
                        pthread_mutex_unlock(&mxPeers);
                        /* multicast params to this player */
                        TextMsg mp = {.kind = PKT_MC_PARAM};
                        strcpy(mp.msg, QUIZ_MC_GRP);
                        uc_send(pl->fd, &mp, sizeof mp);
                        strcpy(mp.msg, TOSTRING(QUIZ_MC_PORT));
                        uc_send(pl->fd, &mp, sizeof mp);
                    } else if (strchr("1234AaBbCcDd", c)) {
                        if (c == '1' || c == 'A' || c == 'a') pl->answer = 1;
                        else if (c == '2' || c == 'B' || c == 'b') pl->answer = 2;
                        else if (c == '3' || c == 'C' || c == 'c') pl->answer = 3;
                        else pl->answer = 4;
                    } else pl->answer = -1;
                }
                break;
            case PKT_PING: /* client ACK */
                recv(pl->fd, &ack, sizeof ack, 0);
                pl->lastSeen = time(NULL);
                printf("[SVR] got keep-alive ACK from (id=%d)\n", pl->id);
                break;
            default: /* drain unknown */
                recv(pl->fd, &kind, sizeof kind, 0);
        }
    }
    drop_player(pl);
    return NULL;
}
/* ─────────────  Accept new TCP clients  ────────────────────────────── */
static void *thr_accept(void *arg) {
    (void) arg;
    int i;
    struct sockaddr_in cliAddr;
    char ipstr[INET_ADDRSTRLEN];
    while (1) {
        socklen_t cliLen = sizeof cliAddr;
        int cfd = accept(g_sockTCP,
                         (struct sockaddr*)&cliAddr,
                         &cliLen);
        if (cfd < 0) continue;
        inet_ntop(AF_INET, &cliAddr.sin_addr,
                  ipstr, sizeof ipstr);
        pthread_mutex_lock(&mxPeers);
        /* (2) kick any stale session with the same IP ------------- */
        for (i = 0; i < g_peerCnt; i++) {
            Player *old = g_players[i];
            if (old && strcmp(old->ip, ipstr) == 0) {
                drop_player(old);                         /* removes + unlocks */
                break;                                    /* at most one */
            }
        }
        if (g_peerCnt >= MAX_PEERS) {
            close(cfd);
            pthread_mutex_unlock(&mxPeers);
            continue;
        }
        Player *pl = calloc(1, sizeof *pl);
        pl->fd = cfd;
        pl->id = g_nextID++;
        pl->answer = -1;
        pl->score = 0;
        pl->ready = false;
        pl->inGame = false;
        pl->lastSeen = time(NULL);
        pl->inLobby = true;
        g_players[g_peerCnt++] = pl;
        pthread_mutex_unlock(&mxPeers);
        pthread_t tRx, tPing;
        pthread_create(&tRx, NULL, thr_player_rx, pl);
        pthread_detach(tRx);
        pthread_create(&tPing, NULL, thr_keepalive, pl);
        pthread_detach(tPing);
        printf("New connection, id=%d\n", pl->id);
    }
}
/* ─────────────  Game logic loop (runs in main thread)  ─────────────── */
static void run_game(void) {
    int i, r, j = 0;
    ScoreBoard sb = {.kind = PKT_SCOREBRD};
    for (r = 0; r < MAX_ROUNDS; r++) {
        /* -------- tell clients how many questions remain ---------- */
        int left = MAX_ROUNDS - r; /* e.g. 10,9,8 … 1 */
        TextMsg cnt = {.kind = PKT_MC_TEXT};
        snprintf(cnt.msg, sizeof cnt.msg,
                 "Questions left: %d", left);
        mc_send(&cnt, sizeof cnt); /* NEW line 1-3 */
        /* reset answers */
        for (i = 0; i < g_peerCnt; i++)
            if (g_players[i] && g_players[i]->inGame) g_players[i]->answer = -1;
        /* send question */
        mc_send(&g_questions[r], sizeof g_questions[r]);
        sleep(5);
        TextMsg txt = {.kind = PKT_MC_TEXT};
        strcpy(txt.msg, "10 seconds left…");
        mc_send(&txt, sizeof txt);
        sleep(10);
        strcpy(txt.msg, "Time’s up!");
        mc_send(&txt, sizeof txt);
        /* evaluate */
        for (i = 0; i < g_peerCnt; i++) {
            Player *pl = g_players[i];
            if (!pl || pl->fd == -1 || !pl->inGame) continue;
            sb.id[j] = pl->id;
            strncpy(sb.name[j], pl->nick, 31);
            if (pl->answer == g_questions[r].right) sb.pts[j] = ++pl->score;
            else sb.pts[j] = pl->score;
            j++;
        }
        sb.nPeers = j;
        mc_send(&sb, sizeof sb);
        j = 0;
        /* NEW: stop the match when only one player remains */
        if (live_ingame()<2) {
            TextMsg win = {.kind = PKT_MC_TEXT};
            g_runningGame = false;
            if (live_ingame() == 1) {
                Player *champ = sole_survivor();
                if (champ) {snprintf(win.msg, sizeof win.msg,
                             "🏆 %s is the last player standing — winner! 🏆",
                             champ->nick);
                    mc_send(&win, sizeof win);
                }
                else {
                    strcpy(win.msg,
                         "All players left — round aborted.\n");
                }
            }
            else {
                strcpy(win.msg,
                        "All players left — round aborted.\n");
            }
            mc_send(&win, sizeof win);
            reset_lobby();           /* ← your existing helper   */
            g_runningGame = false;   /* allow new matches        */
            return;                  /* leave run_game() early   */
        }
        strcpy(txt.msg, "Next round in 5 sec…");
        mc_send(&txt, sizeof txt);
        sleep(5);
    }
}
/* ─────────────  Main  ──────────────────────────────────────────────── */
int main(void) {
    int i;
    srand(time(NULL));
    if (!load_questions("file.txt")) return 1;
    shuffle_questions(MAX_ROUNDS);
    /* TCP listen */
    g_sockTCP = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(g_sockTCP, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
    struct sockaddr_in srv = {AF_INET, htons(QUIZ_TCP_PORT), {INADDR_ANY}};
    bind(g_sockTCP, (struct sockaddr *) &srv, sizeof srv);
    listen(g_sockTCP, TCP_BACKLOG);
    /* UDP multicast sender */
    g_sockMC = socket(AF_INET, SOCK_DGRAM, 0);
    int ttl = 8;
    setsockopt(g_sockMC, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof ttl);
    g_mcAddr.sin_family = AF_INET;
    g_mcAddr.sin_port = htons(QUIZ_MC_PORT);
    g_mcAddr.sin_addr.s_addr = inet_addr(QUIZ_MC_GRP);
    /* accept thread */
    pthread_t tAccept;
    pthread_create(&tAccept,NULL, thr_accept,NULL);
    puts("Quiz-server ready.");
    while (1) {
        sleep(4);
        if (g_readyPeers >= 2 && !g_runningGame) {
            TextMsg soon = {.kind = PKT_MC_TEXT};
            strcpy(soon.msg,
                   "At least two players ready. Starting game in 5 seconds… (others can still join)");
            mc_send(&soon, sizeof soon);
            sleep(5);
            // before you send the first question:
            pthread_mutex_lock(&mxPeers);
            for (i = 0; i < g_peerCnt; i++) {
                Player *pl = g_players[i];
                if (pl->ready) {
                    pl->inGame = true;    // include in this match
                    pl->score  = 0;       // reset their score
                } else {
                    pl->inGame = false;   // not in this match
                }
                pl->ready = false;       // clear for next round
            }
            g_runningGame = true;
            pthread_mutex_unlock(&mxPeers);
            sleep(0.5);
            TextMsg t = {.kind = PKT_MC_TEXT};
            strcpy(t.msg, "Game is starting!");
            mc_send(&t, sizeof t);
            run_game();
            /* -------- declare overall winner (highest score) -------- */
            int topPts = -1;
            Player *champ = NULL;
            for (i = 0; i < g_peerCnt; i++) {
                Player *pl = g_players[i];
                if (pl && pl->fd != -1 && !pl->inLobby) {
                    /* contestants only   */
                    if (pl->score > topPts) {
                        topPts = pl->score;
                        champ = pl;
                    }
                }
            }
            if (champ) {
                TextMsg win = {.kind = PKT_MC_TEXT};
                snprintf(win.msg, sizeof win.msg,
                         "🏆 %s wins the match with %d points! 🏆",
                         champ->nick, champ->score);
                mc_send(&win, sizeof win);
                /* also send by unicast so spectators who never joined MC see it */
                for (i = 0; i < g_peerCnt; i++)
                    if (g_players[i] && g_players[i]->fd != -1)
                        uc_send(g_players[i]->fd, &win, sizeof win);
            }
            g_runningGame = false;
            TextMsg endhdr = { .kind = MSG_GAME_OVER};
            mc_send(&endhdr, sizeof endhdr);
            reset_lobby(); /* ← NEW — prepare for next match */
        }
    }
    return 0;
}

