export class Campaign {
  id: number;
  name: string;
  alias: string;
  date_added: Date;
  hit_limit_for_optimization: number;
  offers: Array<number>;
  optimization_paused: boolean;
  optimize: boolean;
  paused_offers: Array<number>;
  slicing_attrs: Array<string>;
}
