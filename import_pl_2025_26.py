import argparse
import hashlib
import json
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd


def stable_int(seed: str, mod: int) -> int:
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(h[:12], 16) % mod


def pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def map_position(pos_raw) -> str:
    if isinstance(pos_raw, (int, float)) and pd.notna(pos_raw):
        return {1: "GK", 2: "DF", 3: "MF", 4: "FW"}.get(int(pos_raw), "MF")
    p = str(pos_raw or "").strip().upper()
    if p in ("GKP", "GK", "GOALKEEPER"):
        return "GK"
    if p in ("DEF", "DF", "DEFENDER"):
        return "DF"
    if p in ("MID", "MF", "MIDFIELDER"):
        return "MF"
    if p in ("FWD", "FW", "FORWARD", "ST"):
        return "FW"
    if p.startswith("GK"):
        return "GK"
    if p.startswith("D"):
        return "DF"
    if p.startswith("M"):
        return "MF"
    if p.startswith("F") or p.startswith("S"):
        return "FW"
    return "MF"


def derive_birth_year(row: pd.Series) -> Optional[int]:
    for key in ("birthYear", "birth_year", "year_of_birth", "dob_year"):
        if key in row and pd.notna(row[key]):
            try:
                return int(row[key])
            except Exception:
                pass

    for key in ("birth_date", "birthdate", "date_of_birth", "dob"):
        if key in row and pd.notna(row[key]):
            s = str(row[key])
            for sep in ("-", "/", "."):
                if sep in s and len(s) >= 4:
                    try:
                        return int(s.split(sep)[0])
                    except Exception:
                        continue
            if len(s) == 4 and s.isdigit():
                return int(s)
    return None


def generate_rating(player_id: str, team_strength: int, pos: str) -> int:
    ts = max(1, min(int(team_strength), 5))
    team_base = {1: 178, 2: 188, 3: 198, 4: 210, 5: 222}[ts]
    pos_bias = {"GK": -4, "DF": -2, "MF": 0, "FW": +2}.get(pos, 0)
    noise = stable_int(f"{player_id}:rating", 25)
    rating = team_base + pos_bias + noise
    return max(120, min(rating, 238))


def generate_development(player_id: str, pos: str) -> int:
    dev_noise = stable_int(f"{player_id}:dev", 120)
    dev = 80 + dev_noise
    if pos == "GK":
        dev = max(60, dev - 10)
    return max(0, min(dev, 250))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="Path to cloned FPL-Core-Insights repo")
    ap.add_argument("--season", default="2025-2026", help="Season folder under /data/")
    ap.add_argument("--out", default="data/premier_league_2025_26.json", help="Output json path")
    ap.add_argument("--commit", default=None, help="Pinned commit hash/tag")
    args = ap.parse_args()

    source = Path(args.source)
    season_dir = source / "data" / args.season

    players_path = season_dir / "players.csv"
    teams_path = season_dir / "teams.csv"

    if not players_path.exists():
        raise SystemExit(f"Missing {players_path}. Check season folder name.")
    if not teams_path.exists():
        raise SystemExit(f"Missing {teams_path}. Check season folder name.")

    players = pd.read_csv(players_path)
    teams = pd.read_csv(teams_path)

    player_id_col = pick_col(players, ["player_id", "id", "element", "element_id"])
    team_fk_col = pick_col(players, ["team_code", "team", "team_id"])
    pos_col = pick_col(players, ["position", "element_type", "pos"])

    if player_id_col is None:
        raise SystemExit("No player id column found (tried player_id/id/element/element_id).")
    if team_fk_col is None:
        raise SystemExit("No team column found (tried team_code/team/team_id).")
    if pos_col is None:
        raise SystemExit("No position column found (tried position/element_type/pos).")

    web_col = pick_col(players, ["web_name", "name"])
    first_col = pick_col(players, ["first_name"])
    second_col = pick_col(players, ["second_name", "last_name", "surname"])

    team_code_col = pick_col(teams, ["code", "team_code"])
    team_id_col = pick_col(teams, ["id", "team_id"])
    team_name_col = pick_col(teams, ["name"])
    team_short_col = pick_col(teams, ["short_name", "short"])
    strength_col = pick_col(teams, ["strength", "overall_strength"])

    team_strength: Dict[Any, int] = {}
    team_names: Dict[Any, Dict[str, Any]] = {}

    for _, r in teams.iterrows():
        key = r[team_code_col] if team_code_col else r[team_id_col]
        strength = int(r[strength_col]) if (strength_col and pd.notna(r[strength_col])) else 3
        team_strength[key] = strength
        team_names[key] = {
            "name": str(r[team_name_col]) if team_name_col else str(key),
            "short": str(r[team_short_col]) if team_short_col else None,
        }

    out_players = []
    missing_birthyear = 0

    for _, r in players.iterrows():
        pid = str(r[player_id_col])
        team_key = r[team_fk_col]
        pos = map_position(r[pos_col])

        by = derive_birth_year(r)
        if by is None:
            missing_birthyear += 1

        if web_col and pd.notna(r.get(web_col)):
            name = str(r[web_col]).strip()
        else:
            fn = str(r[first_col]).strip() if first_col and pd.notna(r.get(first_col)) else ""
            sn = str(r[second_col]).strip() if second_col and pd.notna(r.get(second_col)) else ""
            name = (fn + " " + sn).strip() if (fn or sn) else f"Player {pid}"

        t_strength = team_strength.get(team_key, 3)
        rating = generate_rating(pid, t_strength, pos)
        dev = generate_development(pid, pos)

        nat = r.get("nationality") if "nationality" in players.columns else None

        out_players.append(
            {
                "name": name,
                "birthYear": by,
                "nationality": nat,
                "position": pos,
                "rating": int(rating),
                "development": int(dev),
                "source": {"playerId": pid, "teamKey": str(team_key)},
            }
        )

    payload = {
        "meta": {
            "season": "2025/2026",
            "currency": "EUR",
            "language": "en",
            "sourceRepo": "olbauday/FPL-Core-Insights",
            "sourceSeasonFolder": args.season,
            "pinnedCommit": args.commit,
            "notes": [
                f"birthYear missing for {missing_birthyear} players (set null); enrich later if needed."
            ],
        },
        "teams": team_names,
        "players": out_players,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out_path} with {len(out_players)} players. birthYear missing: {missing_birthyear}")


if __name__ == "__main__":
    main()
