import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Client } from '@elastic/elasticsearch';

@Injectable()
export class AnalyticsService implements OnModuleInit {
  private readonly logger = new Logger(AnalyticsService.name);
  private client: Client;

  onModuleInit() {
    const host = process.env.ES_HOST || 'localhost';
    const port = process.env.ES_PORT || '9200';
    this.client = new Client({ node: `http://${host}:${port}` });
    this.logger.log(`Elasticsearch client configured → ${host}:${port}`);
  }

  // ─── Health ──────────────────────────────────────────────
  async checkHealth(): Promise<{ status: string; cluster: string; indices: Record<string, number> }> {
    try {
      const health = await this.client.cluster.health();
      const playerCount = await this.safeCount('dota2_player_stats');
      const heroCount = await this.safeCount('dota2_hero_stats');
      return {
        status: 'ok',
        cluster: health.cluster_name,
        indices: {
          dota2_player_stats: playerCount,
          dota2_hero_stats: heroCount,
        },
      };
    } catch (err) {
      this.logger.error('ES health check failed', err);
      return { status: 'error', cluster: 'unavailable', indices: {} };
    }
  }

  // ─── Hero Stats ──────────────────────────────────────────
  async getHeroStats(): Promise<any[]> {
    try {
      const { hits } = await this.client.search({
        index: 'dota2_hero_stats',
        size: 200,
        sort: [{ total_picks: { order: 'desc' } }],
      });
      return hits.hits.map((h: any) => ({ id: h._id, ...h._source }));
    } catch (err) {
      this.logger.error('Failed to fetch hero stats', err);
      return [];
    }
  }

  // ─── Player Stats (aggregated from player_stats index) ──
  async getPlayerStats(): Promise<any[]> {
    try {
      const result = await this.client.search({
        index: 'dota2_player_stats',
        size: 0,
        aggs: {
          players: {
            terms: { field: 'player_name', size: 50, order: { avg_kda: 'desc' } },
            aggs: {
              avg_kills: { avg: { field: 'kills' } },
              avg_deaths: { avg: { field: 'deaths' } },
              avg_assists: { avg: { field: 'assists' } },
              avg_kda: { avg: { field: 'kda' } },
              avg_gpm: { avg: { field: 'gpm' } },
              avg_xpm: { avg: { field: 'xpm' } },
              total_wins: {
                filter: { term: { result: 'WIN' } },
              },
              favorite_hero: {
                terms: { field: 'hero_name', size: 1 },
              },
            },
          },
        },
      });

      const buckets = (result.aggregations?.players as any)?.buckets || [];
      return buckets.map((b: any) => ({
        player_name: b.key,
        games_played: b.doc_count,
        wins: b.total_wins?.doc_count || 0,
        win_rate: b.doc_count > 0 ? Math.round((b.total_wins?.doc_count || 0) / b.doc_count * 100 * 100) / 100 : 0,
        avg_kills: Math.round((b.avg_kills?.value || 0) * 100) / 100,
        avg_deaths: Math.round((b.avg_deaths?.value || 0) * 100) / 100,
        avg_assists: Math.round((b.avg_assists?.value || 0) * 100) / 100,
        avg_kda: Math.round((b.avg_kda?.value || 0) * 100) / 100,
        avg_gpm: Math.round(b.avg_gpm?.value || 0),
        avg_xpm: Math.round(b.avg_xpm?.value || 0),
        favorite_hero: b.favorite_hero?.buckets?.[0]?.key || 'Unknown',
      }));
    } catch (err) {
      this.logger.error('Failed to fetch player stats', err);
      return [];
    }
  }

  // ─── Match Overview ──────────────────────────────────────
  async getMatchOverview(): Promise<any> {
    try {
      const result = await this.client.search({
        index: 'dota2_player_stats',
        size: 0,
        aggs: {
          unique_matches: { cardinality: { field: 'match_id' } },
          unique_heroes: { cardinality: { field: 'hero_id' } },
          unique_players: { cardinality: { field: 'player_name' } },
          avg_duration: { avg: { field: 'duration' } },
          avg_kills: { avg: { field: 'kills' } },
          avg_deaths: { avg: { field: 'deaths' } },
          avg_assists: { avg: { field: 'assists' } },
          avg_gpm: { avg: { field: 'gpm' } },
          avg_xpm: { avg: { field: 'xpm' } },
          radiant_wins: {
            filter: {
              bool: {
                must: [
                  { term: { team: 'Radiant' } },
                  { term: { result: 'WIN' } },
                ],
              },
            },
          },
          dire_wins: {
            filter: {
              bool: {
                must: [
                  { term: { team: 'Dire' } },
                  { term: { result: 'WIN' } },
                ],
              },
            },
          },
          total_radiant: {
            filter: { term: { team: 'Radiant' } },
          },
          total_dire: {
            filter: { term: { team: 'Dire' } },
          },
        },
      });

      const aggs: any = result.aggregations;
      const totalMatches = aggs.unique_matches?.value || 0;
      const radiantWins = aggs.radiant_wins?.doc_count || 0;
      const totalRadiant = aggs.total_radiant?.doc_count || 1;

      return {
        total_matches: totalMatches,
        total_events: result.hits.total?.valueOf() || 0,
        unique_heroes: aggs.unique_heroes?.value || 0,
        unique_players: aggs.unique_players?.value || 0,
        avg_duration: Math.round(aggs.avg_duration?.value || 0),
        avg_kills: Math.round((aggs.avg_kills?.value || 0) * 100) / 100,
        avg_deaths: Math.round((aggs.avg_deaths?.value || 0) * 100) / 100,
        avg_assists: Math.round((aggs.avg_assists?.value || 0) * 100) / 100,
        avg_gpm: Math.round(aggs.avg_gpm?.value || 0),
        avg_xpm: Math.round(aggs.avg_xpm?.value || 0),
        radiant_wins: radiantWins,
        dire_wins: aggs.dire_wins?.doc_count || 0,
        radiant_win_rate: totalRadiant > 0 ? Math.round(radiantWins / totalRadiant * 100 * 100) / 100 : 0,
      };
    } catch (err) {
      this.logger.error('Failed to fetch match overview', err);
      return {};
    }
  }

  // ─── Team Distribution ───────────────────────────────────
  async getTeamDistribution(): Promise<any[]> {
    try {
      const result = await this.client.search({
        index: 'dota2_player_stats',
        size: 0,
        aggs: {
          teams: {
            terms: { field: 'team', size: 2 },
            aggs: {
              results: {
                terms: { field: 'result', size: 3 },
              },
            },
          },
        },
      });

      const buckets = (result.aggregations?.teams as any)?.buckets || [];
      return buckets.map((b: any) => ({
        team: b.key,
        total: b.doc_count,
        breakdown: (b.results?.buckets || []).map((r: any) => ({
          result: r.key,
          count: r.doc_count,
        })),
      }));
    } catch (err) {
      this.logger.error('Failed to fetch team distribution', err);
      return [];
    }
  }

  // ─── Helpers ─────────────────────────────────────────────
  private async safeCount(index: string): Promise<number> {
    try {
      const res = await this.client.count({ index });
      return res.count;
    } catch {
      return 0;
    }
  }
}
