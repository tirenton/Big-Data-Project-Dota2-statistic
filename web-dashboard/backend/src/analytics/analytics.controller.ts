import { Controller, Get } from '@nestjs/common';
import { AnalyticsService } from './analytics.service';

@Controller()
export class AnalyticsController {
  constructor(private readonly analyticsService: AnalyticsService) {}

  @Get('health')
  async health() {
    return this.analyticsService.checkHealth();
  }

  @Get('hero-stats')
  async heroStats() {
    return this.analyticsService.getHeroStats();
  }

  @Get('player-stats')
  async playerStats() {
    return this.analyticsService.getPlayerStats();
  }

  @Get('match-overview')
  async matchOverview() {
    return this.analyticsService.getMatchOverview();
  }

  @Get('team-distribution')
  async teamDistribution() {
    return this.analyticsService.getTeamDistribution();
  }
}
