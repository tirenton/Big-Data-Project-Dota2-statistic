import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Enable CORS for local development (Next.js rewrites handle it in production)
  app.enableCors({
    origin: ['http://localhost:3000', 'http://frontend:3000'],
    methods: 'GET,HEAD',
  });

  // Global API prefix
  app.setGlobalPrefix('api');

  const port = process.env.PORT || 4000;
  await app.listen(port);
  console.log(`🎮 Dota 2 Dashboard API running on http://localhost:${port}`);
}
bootstrap();
