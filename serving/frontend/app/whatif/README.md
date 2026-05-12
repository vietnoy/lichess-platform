# What If

A side-by-side replayer for second-guessing a single move in one of your games.

## The user-facing idea

You're looking at a position from a finished game — say, move 12 — and you wonder: *what if I'd played a different move instead?*

The page shows two boards next to each other:

- **Left ("Game line")** — the position after the move you actually played, then the engine plays both sides forward at depth 12.
- **Right ("Your alternative")** — you drag a piece to make a different move; the engine then plays both sides forward from there.

Each board shows the position 6 plies (≈3 full moves) into its future, with an evaluation timeline underneath. The win-probability bar lets you see *which line the engine prefers* after both have unfolded.

It is **not** a deep analysis. The engine is making best-effort responses but it isn't looking deeper just because the line is hypothetical. Think of it as "play this move out for me so I can see what the position looks like in a few moves."

## Why it exists

A normal chess engine tells you "your move was an inaccuracy, the best move was Nf3 — that's worth +0.6". That's a number, not an intuition. The position the engine *would have led to* is invisible.

WhatIf makes that position concrete. You see what the board would have looked like, what pieces would still be alive, who has the initiative. That's the part you actually need to learn from.

## How it works (one paragraph)

The frontend builds the FEN of the position from the game's move history, and you pick the alternative move by dragging on the right board. Both moves — the actual one from the game, and your alternative — are POSTed to `/api/whatif` together. The backend uses python-chess to legally apply each move from the base position, then asks Stockfish for the best response, applies it, asks again, and so on for `plies` half-moves (default 6). It returns two arrays of `{ uci, fen, cp, mate }` — one per ply for each line. The frontend renders both as scrubbable timelines and converts cp to win-probability for the bar.

## What it isn't, and why it sometimes feels slow

- It is **not** a multi-move trainer. It plays best moves on both sides; you can't intervene mid-line.
- The depth is fixed at 12 to keep latency manageable. Each `/api/whatif` request does up to `2 lines × plies × 2 sides = 24 Stockfish evals`. At ~200-500ms each that's 5-12 seconds.
- The "actual" line is recomputed every time you submit, even though it doesn't change for a given (game, ply). A response cache keyed on `(base_fen, actual_uci, plies)` would make second-guessing cheap.
- The eval timeline doesn't currently mark *when the lines diverge from each other* — that's the moment a player would most want to understand.
