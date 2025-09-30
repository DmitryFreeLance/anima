package ru.animabot;

import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

public class BotMain {
    public static void main(String[] args) throws Exception {
        // 1) Стартуем бота
        TelegramBotsApi botsApi = new TelegramBotsApi(DefaultBotSession.class);
        SoulWayBot bot = new SoulWayBot();
        botsApi.registerBot(bot);
        System.out.println("SoulWayBot started as @" + bot.getBotUsername());

        // 2) Вебсервер для Prodamus
        int port = Integer.parseInt(System.getenv().getOrDefault("PRODAMUS_WEBHOOK_PORT", "8080"));
        String secret = System.getenv().getOrDefault("PRODAMUS_SECRET", "");
        ProdamusWebhookServer webhook = new ProdamusWebhookServer(bot, port, secret);
        webhook.start();

        System.out.println("Prodamus webhook listening on port " + port + " at /webhook/prodamus");
    }
}