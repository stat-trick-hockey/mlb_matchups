#!/usr/bin/env python3
"""
MLB Matchup Data Fetcher
Builds a complete JSON blob for all regular-season games 2023–2025
covering: schedules, probable pitchers, season stats, player bios,
recent form (last 14 games), and head-to-head records.

Output: mlb_data.json  (also injected into index.html)

Usage:
    # Full bootstrap (run once):
    python fetch_mlb_data.py --inject index.html

    # Nightly patch — updates probable pitchers for today + next 2 days,
    # refreshes recent form for active players, injects into HTML:
    python fetch_mlb_data.py --patch --inject index.html
"""

import sys, json, time, argparse, os
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

BASE = "https://statsapi.mlb.com/api/v1"
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "MLB-Matchup-Fetcher/1.0"

SEASONS = [2023, 2024, 2025]
MAX_WORKERS = 6
RATE_SLEEP = 0.12   # seconds between bursts

# ── helpers ──────────────────────────────────────────────────────────────────

def get(path, params=None, retries=3):
    url = BASE + path
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                print(f"  ✗ FAILED {url}: {e}")
                return None
            time.sleep(1.5 * (attempt + 1))

def progress(msg):
    print(f"  → {msg}", flush=True)

# ── schedule ──────────────────────────────────────────────────────────────────

def fetch_season_schedule(season):
    """Return list of {gamePk, date, away, home, awayId, homeId, awayPitcherId, homePitcherId, time}"""
    progress(f"Schedule {season}…")
    # Regular season: April 1 – Oct 6
    start = f"{season}-03-20"
    end   = f"{season}-10-07"
    data = get("/schedule", {
        "sportId": 1,
        "startDate": start,
        "endDate": end,
        "gameType": "R",
        "hydrate": "probablePitcher,team",
    })
    if not data:
        return []

    games = []
    for d in (data.get("dates") or []):
        for g in (d.get("games") or []):
            away = g["teams"]["away"]
            home = g["teams"]["home"]
            awayP = away.get("probablePitcher")
            homeP = home.get("probablePitcher")
            games.append({
                "pk":        g["gamePk"],
                "date":      d["date"],
                "away":      away["team"].get("abbreviation", away["team"]["name"]),
                "home":      home["team"].get("abbreviation", home["team"]["name"]),
                "awayId":    away["team"]["id"],
                "homeId":    home["team"]["id"],
                "awayPit":   awayP["id"]   if awayP else None,
                "awayPitN":  awayP["fullName"] if awayP else None,
                "homePit":   homeP["id"]   if homeP else None,
                "homePitN":  homeP["fullName"] if homeP else None,
                "time":      g.get("gameDate",""),
            })
    progress(f"  {len(games)} games in {season}")
    return games

# ── rosters ───────────────────────────────────────────────────────────────────

def fetch_team_roster(team_id, season):
    data = get(f"/teams/{team_id}/roster", {"rosterType": "active", "season": season})
    if not data:
        return []
    hitters = [p for p in (data.get("roster") or [])
               if p.get("position",{}).get("code") not in ("P","TWP")]
    return [{"id": p["person"]["id"], "name": p["person"]["fullName"],
             "pos": p.get("position",{}).get("abbreviation","")} for p in hitters[:9]]

# ── player bio ────────────────────────────────────────────────────────────────

def fetch_player_info(player_id):
    data = get(f"/people/{player_id}")
    if not data:
        return {"batSide":"?","pitchHand":"?"}
    p = (data.get("people") or [{}])[0]
    return {
        "batSide":   p.get("batSide",  {}).get("code","?"),
        "pitchHand": p.get("pitchHand",{}).get("code","?"),
    }

# ── season stats ──────────────────────────────────────────────────────────────

def fetch_hitting_stats(player_id, season):
    data = get(f"/people/{player_id}/stats", {"stats":"season","group":"hitting","season":season})
    if not data:
        return {}
    s = ((data.get("stats") or [{}])[0].get("splits") or [{}])
    s = s[0].get("stat",{}) if s else {}
    return {
        "avg": float(s.get("avg",0) or 0),
        "ops": float(s.get("ops",0) or 0),
        "obp": float(s.get("obp",0) or 0),
        "slg": float(s.get("slg",0) or 0),
        "hr":  int(s.get("homeRuns",0) or 0),
        "rbi": int(s.get("rbi",0) or 0),
        "ab":  int(s.get("atBats",0) or 0),
        "so":  int(s.get("strikeOuts",0) or 0),
    }

def fetch_pitching_stats(player_id, season):
    data = get(f"/people/{player_id}/stats", {"stats":"season","group":"pitching","season":season})
    if not data:
        return {}
    s = ((data.get("stats") or [{}])[0].get("splits") or [{}])
    s = s[0].get("stat",{}) if s else {}
    era  = float(s.get("era","99") or 99)
    whip = float(s.get("whip","99") or 99)
    bf   = int(s.get("battersFaced",0) or 0)
    so   = int(s.get("strikeOuts",0) or 0)
    return {
        "era":  era  if era  < 99 else None,
        "whip": whip if whip < 99 else None,
        "kp9":  float(s.get("strikeoutsPer9Inn",0) or 0),
        "bb9":  float(s.get("walksPer9Inn",0) or 0),
        "ip":   float(s.get("inningsPitched",0) or 0),
        "wins": int(s.get("wins",0) or 0),
        "so":   so,
        "kPct": round(so/bf,3) if bf else None,
    }

def fetch_recent_hitting(player_id, season):
    data = get(f"/people/{player_id}/stats", {
        "stats":"lastXGames","group":"hitting","season":season,"limit":14})
    if not data:
        return {}
    s = ((data.get("stats") or [{}])[0].get("splits") or [{}])
    s = s[0].get("stat",{}) if s else {}
    return {
        "avg": float(s.get("avg",0) or 0),
        "ops": float(s.get("ops",0) or 0),
        "hr":  int(s.get("homeRuns",0) or 0),
    }

def fetch_recent_pitching(player_id, season):
    data = get(f"/people/{player_id}/stats", {
        "stats":"lastXGames","group":"pitching","season":season,"limit":14})
    if not data:
        return {}
    s = ((data.get("stats") or [{}])[0].get("splits") or [{}])
    s = s[0].get("stat",{}) if s else {}
    era = float(s.get("era","99") or 99)
    return {
        "era": era if era < 99 else None,
        "ip":  float(s.get("inningsPitched",0) or 0),
    }

def fetch_h2h(batter_id, pitcher_id):
    data = get(f"/people/{batter_id}/stats", {
        "stats":"vsPlayer","opposingPlayerId":pitcher_id,
        "group":"hitting","sportId":1})
    if not data:
        return None
    splits = (data.get("stats") or [{}])[0].get("splits") or []
    if not splits:
        return None
    s = splits[0].get("stat",{})
    ab = int(s.get("atBats",0) or 0)
    if ab < 3:
        return None
    return {
        "ab":  ab,
        "avg": float(s.get("avg",0) or 0),
        "hr":  int(s.get("homeRuns",0) or 0),
        "ops": float(s.get("ops",0) or 0),
    }

# ── park factors ─────────────────────────────────────────────────────────────

# Static 2024 park factors (wRC+ based, 100 = neutral).
# Source: Fangraphs park factors, updated annually.
# Key: MLB team abbreviation
PARK_FACTORS = {
    "COL": 118, "BOS": 106, "CIN": 106, "TEX": 105, "PHI": 104,
    "MIL": 103, "NYY": 103, "HOU": 102, "CHC": 102, "BAL": 101,
    "DET": 101, "STL": 100, "MIN": 100, "TOR": 100, "LAA": 100,
    "WSH": 99,  "ATL": 99,  "CLE": 99,  "TB":  98,  "OAK": 98,
    "ARI": 98,  "SEA": 97,  "KC":  97,  "NYM": 97,  "SF":  96,
    "MIA": 96,  "PIT": 96,  "LAD": 95,  "CWS": 95,  "SD":  93,
}

# ── platoon splits ────────────────────────────────────────────────────────────

def fetch_platoon_splits(player_id, season, group="hitting"):
    """Fetch L/R splits for a batter or pitcher."""
    data = get(f"/people/{player_id}/stats", {
        "stats": "statSplits",
        "group": group,
        "season": season,
        "sitCodes": "vl,vr",   # vs Left, vs Right
    })
    if not data:
        return {}

    result = {}
    for split in ((data.get("stats") or [{}])[0].get("splits") or []):
        code = split.get("split", {}).get("code", "")
        s    = split.get("stat", {})
        if code == "vl":
            result["vsL"] = {
                "ops": float(s.get("ops", 0) or 0),
                "avg": float(s.get("avg", 0) or 0),
                "kPct": round(int(s.get("strikeOuts",0) or 0) / max(int(s.get("plateAppearances",1) or 1), 1), 3),
                "bbPct": round(int(s.get("baseOnBalls",0) or 0) / max(int(s.get("plateAppearances",1) or 1), 1), 3),
                "ab": int(s.get("atBats", 0) or 0),
            }
        elif code == "vr":
            result["vsR"] = {
                "ops": float(s.get("ops", 0) or 0),
                "avg": float(s.get("avg", 0) or 0),
                "kPct": round(int(s.get("strikeOuts",0) or 0) / max(int(s.get("plateAppearances",1) or 1), 1), 3),
                "bbPct": round(int(s.get("baseOnBalls",0) or 0) / max(int(s.get("plateAppearances",1) or 1), 1), 3),
                "ab": int(s.get("atBats", 0) or 0),
            }
    return result

def fetch_pitcher_platoon(player_id, season):
    """Fetch OPS against / K% against L and R batters for a pitcher."""
    data = get(f"/people/{player_id}/stats", {
        "stats": "statSplits",
        "group": "pitching",
        "season": season,
        "sitCodes": "vl,vr",
    })
    if not data:
        return {}

    result = {}
    for split in ((data.get("stats") or [{}])[0].get("splits") or []):
        code = split.get("split", {}).get("code", "")
        s    = split.get("stat", {})
        bf   = int(s.get("battersFaced", 0) or 0)
        so   = int(s.get("strikeOuts", 0) or 0)
        bb   = int(s.get("baseOnBalls", 0) or 0)
        era  = float(s.get("era", 99) or 99)
        if code in ("vl", "vr"):
            result[code] = {
                "era":   era if era < 99 else None,
                "kPct":  round(so / bf, 3) if bf else None,
                "bbPct": round(bb / bf, 3) if bf else None,
                "bf":    bf,
            }
    return result



def build_dataset(seasons):
    out = {
        "meta":        {"built": date.today().isoformat(), "seasons": seasons},
        "players":     {},   # id → {bio, hitStats, pitStats, recentHit, recentPit, platoonHit, platoonPit}
        "h2h":         {},   # "batterId-pitcherId" → {ab,avg,hr,ops}
        "rosters":     {},   # "teamId-season" → [{id,name,pos,order}]
        "schedule":    {},   # "YYYY-MM-DD" → [game dicts]
        "parkFactors": PARK_FACTORS,
    }

    all_player_ids   = set()
    all_pitcher_ids  = set()
    all_batter_ids   = set()
    team_season_keys = set()

    # ── STEP 1: schedules ────────────────────────────────
    print("\n[1/6] Fetching schedules…")
    for season in seasons:
        games = fetch_season_schedule(season)
        for g in games:
            dt = g["date"]
            out["schedule"].setdefault(dt, []).append({
                "pk":      g["pk"],
                "away":    g["away"],
                "home":    g["home"],
                "awayId":  g["awayId"],
                "homeId":  g["homeId"],
                "awayPit": g["awayPit"],
                "awayPitN":g["awayPitN"],
                "homePit": g["homePit"],
                "homePitN":g["homePitN"],
                "time":    g["time"],
            })
            for pid in [g["awayPit"], g["homePit"]]:
                if pid:
                    all_pitcher_ids.add((pid, season))
                    all_player_ids.add(pid)
            for tid in [g["awayId"], g["homeId"]]:
                team_season_keys.add((tid, season))

    print(f"   {len(out['schedule'])} dates, {len(all_pitcher_ids)} pitcher-seasons, {len(team_season_keys)} team-seasons")

    # ── STEP 2: rosters ──────────────────────────────────
    print("\n[2/6] Fetching rosters…")
    roster_keys = list(team_season_keys)
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_team_roster, tid, season): (tid, season)
                for tid, season in roster_keys}
        for fut in as_completed(futs):
            tid, season = futs[fut]
            key = f"{tid}-{season}"
            roster = fut.result() or []
            # Store lineup order (1-indexed position in roster = batting order proxy)
            for i, b in enumerate(roster):
                b["order"] = i + 1
            out["rosters"][key] = roster
            for b in roster:
                all_batter_ids.add((b["id"], season))
                all_player_ids.add(b["id"])
            done += 1
            if done % 20 == 0:
                print(f"   {done}/{len(roster_keys)} rosters")
            time.sleep(RATE_SLEEP)
    print(f"   {len(out['rosters'])} rosters fetched, {len(all_batter_ids)} batter-seasons")

    # ── STEP 3: player bios + stats ──────────────────────
    print("\n[3/6] Fetching player bios & season stats…")
    all_ids = list(all_player_ids)
    done = 0

    def fetch_player_bundle(pid):
        bio = fetch_player_info(pid)
        return pid, bio

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_player_bundle, pid): pid for pid in all_ids}
        for fut in as_completed(futs):
            pid, bio = fut.result()
            out["players"].setdefault(str(pid), {
                "bio": bio, "hitStats":{}, "pitStats":{},
                "recentHit":{}, "recentPit":{},
                "platoonHit":{}, "platoonPit":{}
            })
            out["players"][str(pid)]["bio"] = bio
            done += 1
            if done % 100 == 0:
                print(f"   {done}/{len(all_ids)} bios")
            time.sleep(RATE_SLEEP / 2)

    print(f"   Fetching season hitting stats…")
    done = 0
    bat_list = list(all_batter_ids)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_hitting_stats, pid, season): (pid, season)
                for pid, season in bat_list}
        for fut in as_completed(futs):
            pid, season = futs[fut]
            s = fut.result() or {}
            out["players"].setdefault(str(pid), {"bio":{},"hitStats":{},"pitStats":{},"recentHit":{},"recentPit":{}})
            out["players"][str(pid)]["hitStats"][str(season)] = s
            done += 1
            if done % 200 == 0:
                print(f"   {done}/{len(bat_list)} hit-stats")
            time.sleep(RATE_SLEEP / 2)

    print(f"   Fetching season pitching stats…")
    done = 0
    pit_list = list(all_pitcher_ids)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_pitching_stats, pid, season): (pid, season)
                for pid, season in pit_list}
        for fut in as_completed(futs):
            pid, season = futs[fut]
            s = fut.result() or {}
            out["players"].setdefault(str(pid), {"bio":{},"hitStats":{},"pitStats":{},"recentHit":{},"recentPit":{}})
            out["players"][str(pid)]["pitStats"][str(season)] = s
            done += 1
            if done % 100 == 0:
                print(f"   {done}/{len(pit_list)} pit-stats")
            time.sleep(RATE_SLEEP / 2)

    # ── STEP 4: recent form ───────────────────────────────
    print("\n[4/6] Fetching recent form (last 14 games)…")
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {}
        for pid, season in bat_list:
            futs[ex.submit(fetch_recent_hitting, pid, season)] = ("hit", pid, season)
        for pid, season in pit_list:
            futs[ex.submit(fetch_recent_pitching, pid, season)] = ("pit", pid, season)

        for fut in as_completed(futs):
            kind, pid, season = futs[fut]
            s = fut.result() or {}
            p = out["players"].setdefault(str(pid), {"bio":{},"hitStats":{},"pitStats":{},"recentHit":{},"recentPit":{}})
            if kind == "hit":
                p["recentHit"][str(season)] = s
            else:
                p["recentPit"][str(season)] = s
            done += 1
            if done % 300 == 0:
                print(f"   {done}/{len(futs)} recent-form")
            time.sleep(RATE_SLEEP / 3)

    # ── STEP 5: H2H (sample — top matchups per game) ─────
    print("\n[5/6] Fetching head-to-head records…")
    # Build unique batter-pitcher pairs from schedule
    pairs = set()
    for date_str, games in out["schedule"].items():
        for g in games:
            # home batters vs away pitcher
            season = int(date_str[:4])
            hrKey = f"{g['homeId']}-{season}"
            arKey = f"{g['awayId']}-{season}"
            if g["awayPit"]:
                for b in out["rosters"].get(hrKey, [])[:5]:
                    pairs.add((b["id"], g["awayPit"]))
            if g["homePit"]:
                for b in out["rosters"].get(arKey, [])[:5]:
                    pairs.add((b["id"], g["homePit"]))

    print(f"   {len(pairs)} unique batter-pitcher pairs")
    done = 0
    pair_list = list(pairs)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_h2h, bat, pit): (bat, pit)
                for bat, pit in pair_list}
        for fut in as_completed(futs):
            bat, pit = futs[fut]
            result = fut.result()
            if result:
                out["h2h"][f"{bat}-{pit}"] = result
            done += 1
            if done % 500 == 0:
                print(f"   {done}/{len(pair_list)} h2h pairs")
            time.sleep(RATE_SLEEP / 2)

    print(f"   {len(out['h2h'])} h2h records stored (minimum 3 AB)")

    # ── STEP 6: platoon splits ────────────────────────────
    print("\n[6/6] Fetching platoon splits (L/R)…")
    done = 0

    # Batter platoon splits
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_platoon_splits, pid, season, "hitting"): (pid, season)
                for pid, season in bat_list}
        for fut in as_completed(futs):
            pid, season = futs[fut]
            splits = fut.result() or {}
            out["players"].setdefault(str(pid), {
                "bio":{}, "hitStats":{}, "pitStats":{},
                "recentHit":{}, "recentPit":{}, "platoonHit":{}, "platoonPit":{}
            })["platoonHit"][str(season)] = splits
            done += 1
            if done % 200 == 0:
                print(f"   {done}/{len(bat_list)} batter platoon splits")
            time.sleep(RATE_SLEEP / 2)

    # Pitcher platoon splits
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_pitcher_platoon, pid, season): (pid, season)
                for pid, season in pit_list}
        for fut in as_completed(futs):
            pid, season = futs[fut]
            splits = fut.result() or {}
            out["players"].setdefault(str(pid), {
                "bio":{}, "hitStats":{}, "pitStats":{},
                "recentHit":{}, "recentPit":{}, "platoonHit":{}, "platoonPit":{}
            })["platoonPit"][str(season)] = splits
            done += 1
            if done % 100 == 0:
                print(f"   {done}/{len(pit_list)+len(bat_list)} platoon splits total")
            time.sleep(RATE_SLEEP / 2)

    print(f"   Platoon splits complete.")
    return out


# ── nightly patch ─────────────────────────────────────────────────────────────

def fetch_schedule_window(start_date, end_date):
    """Fetch schedule + probable pitchers for a date window."""
    data = get("/schedule", {
        "sportId":  1,
        "startDate": start_date.isoformat(),
        "endDate":   end_date.isoformat(),
        "gameType":  "R",
        "hydrate":   "probablePitcher,team",
    })
    if not data:
        return {}

    result = {}
    for d in (data.get("dates") or []):
        games = []
        for g in (d.get("games") or []):
            away  = g["teams"]["away"]
            home  = g["teams"]["home"]
            awayP = away.get("probablePitcher")
            homeP = home.get("probablePitcher")
            games.append({
                "pk":      g["gamePk"],
                "away":    away["team"].get("abbreviation", away["team"]["name"]),
                "home":    home["team"].get("abbreviation", home["team"]["name"]),
                "awayId":  away["team"]["id"],
                "homeId":  home["team"]["id"],
                "awayPit": awayP["id"]       if awayP else None,
                "awayPitN":awayP["fullName"] if awayP else None,
                "homePit": homeP["id"]       if homeP else None,
                "homePitN":homeP["fullName"] if homeP else None,
                "time":    g.get("gameDate",""),
            })
        if games:
            result[d["date"]] = games
    return result


def patch_dataset(existing, days_ahead=2):
    """
    Fast nightly update — only touches:
      1. Schedule entries for today → today+days_ahead  (refreshes probable pitchers)
      2. Season stats + recent form for any NEW pitcher appearing in that window
      3. Recent form (L14) for ALL pitchers seen in the window (stats change daily)
      4. New player bios + H2H for any brand-new pitcher not in the blob
    Leaves the rest of the blob (historical dates, batter stats, rosters) untouched.
    """
    today     = date.today()
    end_date  = today + timedelta(days=days_ahead)
    season    = today.year

    print(f"\n[PATCH] Refreshing schedule {today} → {end_date}…")
    window = fetch_schedule_window(today, end_date)

    if not window:
        print("  No games in window (off-season or API issue). Nothing to patch.")
        return existing

    # Merge schedule — overwrite entries in the window with fresh data
    for dt, games in window.items():
        existing["schedule"][dt] = games
        print(f"  {dt}: {len(games)} game(s)")

    # Collect all pitcher IDs appearing in the window
    window_pitchers = set()
    for dt, games in window.items():
        for g in games:
            for pid in [g.get("awayPit"), g.get("homePit")]:
                if pid:
                    window_pitchers.add(pid)

    print(f"\n[PATCH] {len(window_pitchers)} pitchers in window — refreshing stats…")

    new_pitchers = [pid for pid in window_pitchers
                    if str(pid) not in existing["players"]]
    known_pitchers = [pid for pid in window_pitchers
                      if str(pid) in existing["players"]]

    # Fetch bios for brand-new pitchers
    if new_pitchers:
        print(f"  {len(new_pitchers)} new pitchers — fetching bios + season stats + platoon…")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            bio_futs = {ex.submit(fetch_player_info, pid): pid for pid in new_pitchers}
            for fut in as_completed(bio_futs):
                pid = bio_futs[fut]
                bio = fut.result() or {"batSide":"?","pitchHand":"?"}
                existing["players"].setdefault(str(pid), {
                    "bio": bio, "hitStats":{}, "pitStats":{},
                    "recentHit":{}, "recentPit":{}, "platoonHit":{}, "platoonPit":{}
                })["bio"] = bio
            time.sleep(RATE_SLEEP)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            stat_futs = {ex.submit(fetch_pitching_stats, pid, season): pid for pid in new_pitchers}
            for fut in as_completed(stat_futs):
                pid = stat_futs[fut]
                s = fut.result() or {}
                existing["players"][str(pid)]["pitStats"][str(season)] = s
            time.sleep(RATE_SLEEP)

        # Platoon splits for new pitchers
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(fetch_pitcher_platoon, pid, season): pid for pid in new_pitchers}
            for fut in as_completed(futs):
                pid = futs[fut]
                splits = fut.result() or {}
                existing["players"][str(pid)].setdefault("platoonPit", {})[str(season)] = splits
            time.sleep(RATE_SLEEP)

    # Refresh recent form (L14) for ALL window pitchers — this changes every day
    print(f"  Refreshing recent form for {len(window_pitchers)} pitchers…")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_recent_pitching, pid, season): pid
                for pid in window_pitchers}
        for fut in as_completed(futs):
            pid = futs[fut]
            s = fut.result() or {}
            existing["players"].setdefault(str(pid), {
                "bio":{}, "hitStats":{}, "pitStats":{},
                "recentHit":{}, "recentPit":{}, "platoonHit":{}, "platoonPit":{}
            })["recentPit"][str(season)] = s
        time.sleep(RATE_SLEEP)

    # Also refresh season pitching stats for window pitchers (ERA/WHIP update daily)
    print(f"  Refreshing season stats for {len(window_pitchers)} pitchers…")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_pitching_stats, pid, season): pid
                for pid in window_pitchers}
        for fut in as_completed(futs):
            pid = futs[fut]
            s = fut.result() or {}
            existing["players"][str(pid)]["pitStats"][str(season)] = s
        time.sleep(RATE_SLEEP)

    # Refresh platoon splits for all window pitchers (these shift as sample grows)
    print(f"  Refreshing platoon splits for {len(window_pitchers)} pitchers…")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_pitcher_platoon, pid, season): pid
                for pid in window_pitchers}
        for fut in as_completed(futs):
            pid = futs[fut]
            splits = fut.result() or {}
            existing["players"][str(pid)].setdefault("platoonPit", {})[str(season)] = splits
        time.sleep(RATE_SLEEP)

    # Fetch H2H for any new batter-pitcher pairs introduced by new pitchers
    if new_pitchers:
        new_pairs = set()
        for dt, games in window.items():
            for g in games:
                for pit_id, batting_team_id in [
                    (g.get("awayPit"), g.get("homeId")),
                    (g.get("homePit"), g.get("awayId")),
                ]:
                    if pit_id not in new_pitchers:
                        continue
                    roster_key = f"{batting_team_id}-{season}"
                    for b in existing["rosters"].get(roster_key, [])[:5]:
                        pair_key = f"{b['id']}-{pit_id}"
                        if pair_key not in existing["h2h"]:
                            new_pairs.add((b["id"], pit_id))

        if new_pairs:
            print(f"  Fetching {len(new_pairs)} new H2H pairs…")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = {ex.submit(fetch_h2h, bat, pit): (bat, pit)
                        for bat, pit in new_pairs}
                for fut in as_completed(futs):
                    bat, pit = futs[fut]
                    result = fut.result()
                    if result:
                        existing["h2h"][f"{bat}-{pit}"] = result
                time.sleep(RATE_SLEEP)

    # Update meta
    existing["meta"]["patched"]      = date.today().isoformat()
    existing["meta"]["patch_window"] = f"{today} → {end_date}"

    print(f"\n✅ Patch complete.")
    print(f"   Dates updated: {list(window.keys())}")
    print(f"   Pitchers refreshed: {len(window_pitchers)}")
    return existing


# ── inject into HTML ──────────────────────────────────────────────────────────

def inject_into_html(data, html_path):
    if not os.path.exists(html_path):
        print(f"  ✗ HTML file not found: {html_path}")
        return
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    blob = json.dumps(data, separators=(",",":"), ensure_ascii=True)
    marker_start = "/* __MLB_DATA_START__ */"
    marker_end   = "/* __MLB_DATA_END__ */"
    replacement  = f"{marker_start}\nconst MLB_DATA = {blob};\n{marker_end}"

    if marker_start in html and marker_end in html:
        # Plain string slice — avoids regex choking on \u escapes in the blob
        start_idx = html.index(marker_start)
        end_idx   = html.index(marker_end) + len(marker_end)
        html = html[:start_idx] + replacement + html[end_idx:]
    else:
        html = html.replace("/* __INSERT_DATA_HERE__ */", replacement)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    size_kb = len(blob) / 1024
    print(f"  ✔ Injected {size_kb:.0f} KB into {html_path}")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons",    nargs="+", type=int, default=SEASONS)
    parser.add_argument("--inject",     type=str,  default="", help="HTML file to inject data into")
    parser.add_argument("--out",        type=str,  default="mlb_data.json", help="Output JSON path")
    parser.add_argument("--patch",      action="store_true",
                        help="Fast nightly mode: refresh probable pitchers + recent form only")
    parser.add_argument("--days-ahead", type=int,  default=2,
                        help="How many days ahead to patch probable pitchers (default: 2)")
    args = parser.parse_args()

    t0 = time.time()

    if args.patch:
        # ── NIGHTLY PATCH MODE ──────────────────────────────
        print(f"\n🔄 PATCH MODE — refreshing probable pitchers + recent form")
        if not os.path.exists(args.out):
            print(f"❌ {args.out} not found. Run without --patch first to build the full dataset.")
            sys.exit(1)

        with open(args.out, "r", encoding="utf-8") as f:
            existing = json.load(f)

        dataset = patch_dataset(existing, days_ahead=args.days_ahead)

    else:
        # ── FULL BOOTSTRAP MODE ─────────────────────────────
        print(f"\n🔄 FULL BUILD — fetching MLB data for seasons: {args.seasons}")
        dataset = build_dataset(args.seasons)

    elapsed = time.time() - t0
    print(f"\n✅ Done in {elapsed:.0f}s")
    print(f"   Dates:   {len(dataset['schedule'])}")
    print(f"   Players: {len(dataset['players'])}")
    print(f"   H2H:     {len(dataset['h2h'])}")
    print(f"   Rosters: {len(dataset['rosters'])}")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(dataset, f, separators=(",",":"), ensure_ascii=True)
    size_kb = os.path.getsize(args.out) / 1024
    print(f"   Saved:   {args.out} ({size_kb:.0f} KB)")

    if args.inject:
        inject_into_html(dataset, args.inject)
