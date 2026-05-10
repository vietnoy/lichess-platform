export type Side = "white" | "black";

export interface GameMeta {
  white_id: string;
  black_id: string;
  white_rating: number;
  black_rating: number;
  opening_eco: string | null;
  opening_name: string | null;
  speed: string;
  winner: Side | null;
  end_status: string | null;
}

export interface Move {
  ply: number;
  side: Side;
  san: string;
  fen: string;
  clock_s: number | null;
}

export interface Game {
  game_id: string;
  metadata: GameMeta;
  moves: Move[];
}

export interface EvalResult {
  cp: number | null;
  mate: number | null;
  best_move: string | null;
}
