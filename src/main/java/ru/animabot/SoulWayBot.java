package ru.animabot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.api.methods.BotApiMethod;
import org.telegram.telegrambots.meta.api.methods.groupadministration.BanChatMember;
import org.telegram.telegrambots.meta.api.methods.groupadministration.CreateChatInviteLink;
import org.telegram.telegrambots.meta.api.methods.groupadministration.GetChatMember;
import org.telegram.telegrambots.meta.api.methods.send.SendDocument;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.methods.send.SendVideo;
import org.telegram.telegrambots.meta.api.objects.*;
import org.telegram.telegrambots.meta.api.objects.chatmember.ChatMember;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.meta.api.objects.InputFile;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class SoulWayBot extends TelegramLongPollingBot {

    private static final Logger LOG = LoggerFactory.getLogger(SoulWayBot.class);

    // ===== Настройки (ключи в БД) =====
    private static final String S_WELCOME_TEXT     = "welcome_text";
    private static final String S_WELCOME_VIDEO    = "welcome_video";
    private static final String S_CLUB_TEXT        = "club_text";
    private static final String S_REVIEWS_TEXT     = "reviews_text";
    private static final String S_REVIEWS_URL      = "reviews_url";
    private static final String S_ABOUT_TEXT       = "about_text";
    private static final String S_SESSIONS_TEXT    = "sessions_text";
    private static final String S_SESSIONS_URL     = "sessions_url";
    private static final String S_PROCVETA_TEXT    = "procveta_text";
    private static final String S_PROCVETA_URL     = "procveta_url";

    // Тарифы: лейбл/дни/база-URL (поддомен или поддомен+реф)
    private static final String S_TAR1_LABEL = "tariff1_label";
    private static final String S_TAR1_DAYS  = "tariff1_days";
    private static final String S_TAR1_URL   = "tariff1_url";

    private static final String S_TAR2_LABEL = "tariff2_label";
    private static final String S_TAR2_DAYS  = "tariff2_days";
    private static final String S_TAR2_URL   = "tariff2_url";

    private static final String S_TAR3_LABEL = "tariff3_label";
    private static final String S_TAR3_DAYS  = "tariff3_days";
    private static final String S_TAR3_URL   = "tariff3_url";

    private static final String S_GROUP_ID         = "group_id";
    private static final String S_GROUP_INVITE_URL = "group_invite_url";
    private static final String S_GIFT_KEYWORD     = "gift_keyword";

    // ===== ENV =====
    private final String BOT_TOKEN      = System.getenv().getOrDefault("TG_BOT_TOKEN", "YOUR_TOKEN");
    private final String BOT_USERNAME   = System.getenv().getOrDefault("TG_BOT_USERNAME", "SoulWayClub_bot");
    private final long   ADMIN_ID       = Long.parseLong(System.getenv().getOrDefault("TG_ADMIN_ID", "726773708"));
    private final String CHANNEL_ID     = System.getenv().getOrDefault("TG_CHANNEL_ID", "sibirskaiapro"); // без @
    private final String BOT_LINK_SECRET= System.getenv().getOrDefault("BOT_LINK_SECRET", "");

    /** Корень локальных материалов внутри контейнера. */
    private static final String FILES_ROOT = "/work/files/";

    private final SQLiteManager db;
    private final SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // ===== Callback-ключи =====
    private static final String CB_CHECKSUB_PREFIX = "CHECKSUB:";
    private static final String CB_OPENKW_PREFIX   = "OPENKW:";
    private static final String CB_MENU_CLUB   = "MENU:CLUB";
    private static final String CB_MENU_TARIFF = "MENU:TARIFFS";
    private static final String CB_MENU_REV    = "MENU:REVIEWS";
    private static final String CB_MENU_ABOUT  = "MENU:ABOUT";
    private static final String CB_MENU_SESS   = "MENU:SESSIONS";
    private static final String CB_MENU_SUB    = "MENU:SUB";
    private static final String CB_MENU_PROCV  = "MENU:PROCVETA";
    private static final String CB_MENU_BACK   = "MENU:BACK";

    // ===== Тексты drip-рассылки =====
    private static final String DRIP_DAY1 =
            "Всю свою осознанную жизнь я страдала…\n\n" +
                    "Что я имею ввиду, когда происходили классные события, да, я за них благодарила, но не принимала, думала что не достойна, иногда обесценивала.\n\n" +
                    "Например, я полетела в Доминикану, красота… море, пляж, все как на сказочной картинке, и вот я помню, захожу в теплый океан, солнце не жарит, так как есть облачка, пальмы вокруг, одним словом - счастье и внутренне умиротворение…\n\n" +
                    "Что случилось потом?\nЯ подумала, что так нельзя, нельзя, чтобы было так хорошо, некомфортно когда хорошо, представляете?\n\n" +
                    "И все, исполнено!\n\nЯ заболела, плюс на Доминикану тогда надвигался ураган, передавали по всем новостям, что снесет все отели… я оставшиеся дни, в стрессе просто хотела улететь домой (благодарю до сих пор, что за день до урагана у меня случился вылет), но сам факт того, что внутренне мне стало комфортнее быть в стрессе, чем в спокойствии.\n\n" +
                    "Так вот что хочу сказать, страдать мы умеем, выживать - мы умеем, а находится в гармоничном состоянии - этому надо учится.\n\n" +
                    "И выбор за вами - хотите вы страдать или нет.\n\n" +
                    "Начать надо с того, чтобы осознать и сделать выбор - чего вы хотите на самом деле, признаться себе.\n\n" +
                    "А в закрытом клубе я даю поддерживающее поле, где происходит изменения на многих уровнях… меняется мышление, окружение и усиливается связь со своей ДУШОЙ.\n\n" +
                    "PS Могу сказать, что в гармоничном состоянии жить намного приятнее, чувствовать вкус жизни радостным и ярким - этому можно научится.\n\n" +
                    "Приглашаю!\n\n*По дополнительным вопросам: @soulwaycare.";

    private static final String DRIP_DAY3 =
            "Вспомни фразу, как встретишь Новый год, так его и проведешь.\n\n" +
                    "Так можно сказать про день - как встретишь, так и проведешь…\n\n" +
                    "И это правда, настрой на день закладывает все события дня.\n\n" +
                    "В закрытом клубе уже доступны медитации для гармоничного начала дня:\n\n" +
                    "«Настрой на прекрасный день» - для создания вокруг себя пространства любви\n" +
                    "«Энергетическая защита» - для создания защитного кокона, чтобы уберечь свое настроение и состояние от недоброжелателей\n" +
                    "«Состояние изобилия» - эта практика хороша как утром, так и в течении дня в любое время\n\n" +
                    "Присоединяйся!\n\n*По дополнительным вопросам: @soulwaycare.";

    private static final String DRIP_DAY7 =
            "Почему у одних все получается, а у других постоянно «не сейчас»?\n\n" +
                    "Ведь и мотивация есть, и желание, в чем подвох?\n\n" +
                    "Порой дело в твоем окружении, в какой среде ты находишься.\n\n" +
                    "Представь себе красивый цветок, розу… и ты ее можешь поливать, пересаживать в кашпо, покупать удобрения, но вокруг - холод, темно, и нет солнца - она не зацветет…\n\n" +
                    "Так происходит и с нами.\n\n" +
                    "Если ты умная, красивая, у тебя есть вдохновение и мотивация, но рядом лишь те, кто не верит и обесценивает, то шанс на успех крайне мал.\n\n" +
                    "Проверила на себе, знаю о чем говорю… \nХочешь действительно расцвести?\nНужно найти свою «теплицу»\n\n" +
                    "Клуб Путь Души - это больше, чем клуб\n\n" +
                    "Здесь есть все для того, чтобы вырасти:\n\n" +
                    "Мощные практики - для ресурсного состояния и нового мышления\n" +
                    "Совместные медитации - активации потенциала\n" +
                    "Окружение - где понимают тебя и поддерживают\n\n" +
                    "И если ты давно в поиске такого общения - сегодня идеальный момент.\n\n" +
                    "Присоединяйся!\n\n*По дополнительным вопросам: @soulwaycare.";

    public SoulWayBot() {
        db = new SQLiteManager("soulway.db");
        df.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"));
        seedDefaults();

        // Периодическая чистка и рассылка
        scheduler.scheduleAtFixedRate(() -> {
            try { cleanupExpired(); } catch (Exception e) { LOG.warn("cleanup error", e); }
        }, 30, 30, TimeUnit.MINUTES);

        scheduler.scheduleAtFixedRate(() -> {
            try { processDripCampaigns(); } catch (Exception e) { LOG.warn("drip error", e); }
        }, 1, 10, TimeUnit.MINUTES);
    }

    public SQLiteManager getDb() { return db; }

    // ===== HMAC и токен заказа =====

    public static String hmacHex(String data, String secret) {
        try {
            if (secret == null || secret.isBlank()) return "0";
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] out = mac.doFinal(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(out.length * 2);
            for (byte b : out) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    /** swb:<uid>:<days>:<hmac(uid:days)> */
    private String buildOrderIdToken(long uid, int days) {
        String payload = uid + ":" + days;
        return "swb:" + payload + ":" + hmacHex(payload, BOT_LINK_SECRET);
    }

    /** Разбор нашего токена. null если подпись не сходится. */
    public static long[] parseOrderIdToken(String token, String secret) {
        if (token == null) return null;
        String t = token.trim();
        // если пришёл URL-кодированный — декодируем
        try { t = java.net.URLDecoder.decode(t, java.nio.charset.StandardCharsets.UTF_8); } catch (Exception ignore) {}
        if (!t.startsWith("swb:")) return null;
        String[] p = t.split(":", 4);
        if (p.length != 4) return null;
        try {
            long uid = Long.parseLong(p[1]);
            int days = Integer.parseInt(p[2]);
            if (!Objects.equals(hmacHex(uid + ":" + days, secret), p[3])) return null;
            return new long[]{uid, days};
        } catch (Exception ignore) { return null; }
    }

    // ===== Дефолтные значения =====

    private void seedDefaults() {
        putIfEmpty(S_WELCOME_TEXT,
                "{name}, Добро пожаловать!\n" +
                        "Получи свой подарок, нажав на кодовое слово ниже, а Видео\n\n" +
                        "Dmitry, приветствую тебя!\n" +
                        "Меня зовут Анна Сибирская.\n" +
                        "Приглашаю тебя в закрытый клуб Путь души | Soul Way.\n\n" +
                        "Я создала это пространство для глубокого погружения и слияния со своей Душой, для раскрытия своей силы, потенциалов и новых возможностей ✨\n\n" +
                        "Хочешь почувствовать вкус жизни, энергию, вдохновение?\n\n" +
                        "Присоединяйся ❤️\n\n" +
                        "Посмотреть наполнение клуба можно по кнопке «О Клубе» ниже⤵️\n\n" +
                        "По вопросам: @SoulWayCare");
        putIfEmpty(S_GIFT_KEYWORD, "СВОБОДА");

        putIfEmpty(S_CLUB_TEXT,
                "📘 О КЛУБЕ\n\n" +
                        "✅ Доступ к записи трёх родовых практик\n" +
                        "✅ Медитации «Энергетическая защита» и «Состояние Изобилия»\n" +
                        "✅ 2 онлайн-встречи с ченнелинг-медитациями\n" +
                        "✅ Курс «Выбери Силу» (18 уроков)\n" +
                        "✅ Ежедневные аффирмации, чат поддержки и ответы на вопросы\n" +
                        "Бонус: скидка 40% на личную сессию");

        putIfEmpty(S_REVIEWS_TEXT, "📝 ОТЗЫВЫ\nЗдесь вы можете посмотреть отзывы:");
        putIfEmpty(S_REVIEWS_URL,  "https://t.me/sibirskaiapro/336");
        putIfEmpty(S_ABOUT_TEXT,
                "👤 ОБО МНЕ\n\n" +
                        "Меня зовут Анна Сибирская — энерготерапевт, парапсихолог, ченнелер, " +
                        "ведущая трансформационных игр и художница энергетических картин.\n" +
                        "Создала клубы «Процветай» и «Путь Души (Soul Way)».\n");
        putIfEmpty(S_SESSIONS_TEXT,"🧘‍♀️ МОИ СЕАНСЫ\nЗдесь Вы можете ознакомиться с моими сеансами:");
        putIfEmpty(S_SESSIONS_URL, "https://t.me/sibirskaiapro/65");
        putIfEmpty(S_PROCVETA_TEXT,"🌸 КЛУБ «ПРОЦВЕТАЙ»\nКлуб с живыми встречами в Санкт-Петербурге ❤️");
        putIfEmpty(S_PROCVETA_URL, "https://t.me/procvetaiclub");

        // Рекомендую поддоменную форму или поддомен+реф (оба варианта поддерживаются)
        putIfEmpty(S_TAR1_LABEL, "1 МЕС • 1299 ₽");
        putIfEmpty(S_TAR1_DAYS,  "30");
        putIfEmpty(S_TAR1_URL,   "https://soulway.payform.ru/4e9isVQ/");

        putIfEmpty(S_TAR2_LABEL, "3 МЕС • 3599 ₽");
        putIfEmpty(S_TAR2_DAYS,  "90");
        putIfEmpty(S_TAR2_URL,   "https://soulway.payform.ru/en9it1j/");

        putIfEmpty(S_TAR3_LABEL, "12 МЕС • 12900 ₽");
        putIfEmpty(S_TAR3_DAYS,  "365");
        putIfEmpty(S_TAR3_URL,   "https://soulway.payform.ru/kr9it4z/");
    }

    private void putIfEmpty(String key, String value) {
        String cur = db.getSetting(key, null);
        if (cur == null || cur.isBlank()) db.setSetting(key, value);
    }

    // ===== Telegram API =====

    @Override public String getBotUsername() { return BOT_USERNAME; }
    @Override public String getBotToken() { return BOT_TOKEN; }

    @Override
    public void onUpdateReceived(Update update) {
        try {
            if (update == null) return;
            if (update.hasMessage()) handleMessage(update.getMessage());
            else if (update.hasCallbackQuery()) handleCallback(update.getCallbackQuery());
        } catch (Exception e) {
            LOG.error("Error processing update", e);
        }
    }

    private void handleMessage(Message msg) {
        if (msg == null || msg.getFrom() == null) return;
        long chatId = msg.getChatId();
        String text = msg.hasText() ? msg.getText().trim() : "";

        try {
            if (text.startsWith("/")) { handleCommand(msg, text); return; }

            // Убрали ввод кодового слова с клавиатуры:
            sendText(chatId, "Ой, кажется я не знаю такой команды, напишите /help если вам нужна помощь.");
        } catch (Exception e) {
            LOG.error("handleMessage error", e);
        }
    }

    private void handleCommand(Message msg, String text) {
        long chatId = msg.getChatId();
        long userId = msg.getFrom().getId();
        String[] parts = text.split(" ", 2);
        String cmd = parts[0].toLowerCase(Locale.ROOT);
        String args = parts.length > 1 ? parts[1] : "";

        try {
            switch (cmd) {
                case "/start": {
                    String raw = db.getSetting(S_WELCOME_TEXT, "{name}, Добро пожаловать!");
                    String name = msg.getFrom().getFirstName() != null ? msg.getFrom().getFirstName() : "друг";
                    SendMessage sm = new SendMessage(String.valueOf(chatId), raw.replace("{name}", name));
                    sm.setReplyMarkup(buildStartGiftKeyboard());
                    safeExec(sm);
                    break;
                }
                case "/menu": {
                    String firstName = msg.getFrom().getFirstName() != null ? msg.getFrom().getFirstName() : "друг";
                    sendWelcomeWithMenu(chatId, firstName);
                    break;
                }
                case "/help": {
                    String help =
                            "Доступные команды:\n" +
                                    "• /start — приветствие и подарок\n" +
                                    "• /menu — главное меню\n" +
                                    "• /help — список команд\n";
                    sendText(chatId, help);
                    break;
                }
                case "/setgift": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    if (args.isBlank()) { sendText(chatId, "Формат: /setgift <KEYWORD>"); break; }
                    db.setSetting(S_GIFT_KEYWORD, args.trim().toUpperCase(Locale.ROOT));
                    sendText(chatId, "✅ Кодовое слово обновлено: " + args.trim().toUpperCase(Locale.ROOT));
                    break;
                }
                case "/addkw": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    if (args.isBlank()) { sendText(chatId, "Формат: /addkw KEY|INTRO|REWARD|mat1,mat2"); break; }
                    String[] p = args.split("\\|", 4);
                    if (p.length < 3) { sendText(chatId, "Нужно минимум KEY|INTRO|REWARD"); break; }
                    Keyword kw = new Keyword(
                            safeTrim(p[0]).toUpperCase(Locale.ROOT),
                            safeTrim(p[1]), safeTrim(p[2]),
                            (p.length == 4 && p[3] != null && !p[3].isBlank())
                                    ? Arrays.asList(p[3].split(",")) : Collections.emptyList());
                    db.upsertKeyword(kw);
                    sendText(chatId, "✅ Добавлено/обновлено слово: " + kw.getKeyword());
                    break;
                }
                case "/listkw": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    List<Keyword> list = db.listKeywords();
                    if (list.isEmpty()) sendText(chatId, "Список пуст.");
                    else {
                        StringBuilder sb = new StringBuilder("Кодовые слова:\n");
                        for (Keyword k : list) sb.append("• ").append(k.getKeyword()).append("\n");
                        sendText(chatId, sb.toString());
                    }
                    break;
                }

                // Редактируемые тексты/ссылки
                case "/setwelcome":       if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_WELCOME_TEXT, args); sendText(chatId, "✅ Приветственный текст обновлён."); break;
                case "/setwelcomevideo":  if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_WELCOME_VIDEO, args); sendText(chatId, "✅ Видео приветствия обновлено."); break;
                case "/setclub":          if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_CLUB_TEXT, args);     sendText(chatId, "✅ «О клубе» обновлён."); break;
                case "/setreviews":       if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_REVIEWS_URL, args);   sendText(chatId, "✅ Ссылка «Отзывы» обновлена."); break;
                case "/setreviews_text":  if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_REVIEWS_TEXT, args);  sendText(chatId, "✅ Текст «Отзывы» обновлён."); break;
                case "/setabout":         if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_ABOUT_TEXT, args);    sendText(chatId, "✅ «Обо мне» обновлено."); break;
                case "/setsessions":      if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_SESSIONS_URL, args);  sendText(chatId, "✅ Ссылка «Сеансы» обновлена."); break;
                case "/setsessions_text": if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_SESSIONS_TEXT, args); sendText(chatId, "✅ Текст «Сеансы» обновлён."); break;
                case "/setprocveta":      if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_PROCVETA_URL, args);  sendText(chatId, "✅ Ссылка «Процветай» обновлена."); break;
                case "/setprocveta_text": if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_PROCVETA_TEXT, args); sendText(chatId, "✅ Текст «Процветай» обновлён."); break;

                // Тарифы: LABEL|DAYS|BASE_URL
                case "/settariff1": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) { sendText(chatId, "Формат: /settariff1 LABEL|DAYS|BASE_URL"); break; }
                    db.setSetting(S_TAR1_LABEL, a[0].trim());
                    db.setSetting(S_TAR1_DAYS,  a[1].trim());
                    db.setSetting(S_TAR1_URL,   a[2].trim());
                    sendText(chatId, "✅ Тариф #1 сохранён.");
                    break;
                }
                case "/settariff2": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) { sendText(chatId, "Формат: /settariff2 LABEL|DAYS|BASE_URL"); break; }
                    db.setSetting(S_TAR2_LABEL, a[0].trim());
                    db.setSetting(S_TAR2_DAYS,  a[1].trim());
                    db.setSetting(S_TAR2_URL,   a[2].trim());
                    sendText(chatId, "✅ Тариф #2 сохранён.");
                    break;
                }
                case "/settariff3": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) { sendText(chatId, "Формат: /settariff3 LABEL|DAYS|BASE_URL"); break; }
                    db.setSetting(S_TAR3_LABEL, a[0].trim());
                    db.setSetting(S_TAR3_DAYS,  a[1].trim());
                    db.setSetting(S_TAR3_URL,   a[2].trim());
                    sendText(chatId, "✅ Тариф #3 сохранён.");
                    break;
                }

                // Группа/сервис
                case "/setgroup":     if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_GROUP_ID, args.trim());         sendText(chatId, "✅ ID группы сохранён."); break;
                case "/setgrouplink": if (!isAdmin(userId)) { deny(chatId); break; } db.setSetting(S_GROUP_INVITE_URL, args.trim()); sendText(chatId, "✅ Инвайт-ссылка сохранена."); break;

                case "/grantsub": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    String[] a = args.split("\\s+");
                    if (a.length < 2) { sendText(chatId, "Формат: /grantsub <userId> <days>"); break; }
                    long uid = Long.parseLong(a[0]);
                    int days = Integer.parseInt(a[1]);
                    db.grantSubscription(uid, days);
                    sendText(chatId, "✅ Подписка выдана: " + uid + " на " + days + " дн.");
                    String invite = ensureInviteLink();
                    if (invite != null) sendText(uid, "🔗 Ссылка для входа в чат: " + invite);
                    break;
                }
                case "/cleanup": {
                    if (!isAdmin(userId)) { deny(chatId); break; }
                    int n = cleanupExpired();
                    sendText(chatId, "🧹 Удалено из группы: " + n);
                    break;
                }

                default: sendText(chatId, "Ой, кажется я не знаю такой команды, напишите /help если вам нужна помощь.");
            }
        } catch (Exception e) {
            LOG.error("handleCommand error", e);
        }
    }

    private boolean isAdmin(long uid) { return uid == ADMIN_ID; }
    private void deny(long chatId) { sendText(chatId, "Доступ только администратору."); }

    private void handleCallback(CallbackQuery cb) {
        if (cb == null) return;
        String data = cb.getData();
        long uid = cb.getFrom().getId();
        long chatId = cb.getMessage() != null ? cb.getMessage().getChatId() : uid;

        try {
            if (data != null && data.startsWith(CB_CHECKSUB_PREFIX)) {
                String key = data.substring(CB_CHECKSUB_PREFIX.length());
                Keyword kw = db.findKeywordByKey(key);
                if (kw == null) { answerCallback(cb.getId(), "Кодовое слово не найдено."); return; }

                // Проверяем подписку на канал
                boolean subscribed = isSubscribedToChannel(uid);
                if (!subscribed) {
                    // Сообщаем и снова показываем кнопки
                    sendText(chatId, "Ой, кажется вы не подписаны на @"+CHANNEL_ID+". Подпишитесь и попробуйте еще раз.");
                    SendMessage sm = new SendMessage(String.valueOf(chatId), nonEmpty(kw.getIntroText(), "🎁 Подарок:"));
                    sm.setReplyMarkup(buildIntroKeyboard(kw));
                    safeExec(sm);
                    answerCallback(cb.getId(), "");
                    return;
                }

                // Всё ок — выдаём бонус
                answerCallback(cb.getId(), "✅ Подписка на канал подтверждена!");
                sendReward(chatId, kw);

                // После бонуса показываем меню
                String firstName = cb.getFrom().getFirstName() != null ? cb.getFrom().getFirstName() : "друг";
                sendWelcomeWithMenu(chatId, firstName);

                // Если платной подписки нет — запускаем drip-рассылку (1,3,7 день)
                Long exp = db.getSubscriptionExpiry(uid);
                if (exp == null || exp <= System.currentTimeMillis()) {
                    db.startOrResetDrip(uid, System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1), 0);
                }

                return;
            }

            if (data != null && data.startsWith(CB_OPENKW_PREFIX)) {
                String key = data.substring(CB_OPENKW_PREFIX.length());
                Keyword kw = db.findKeywordByKey(key);
                if (kw == null) { answerCallback(cb.getId(), "Кодовое слово не найдено."); return; }
                SendMessage sm = new SendMessage(String.valueOf(chatId), nonEmpty(kw.getIntroText(), "🎁 Подарок:"));
                sm.setReplyMarkup(buildIntroKeyboard(kw));
                safeExec(sm);
                answerCallback(cb.getId(), "");
                return;
            }

            switch (data) {
                case CB_MENU_CLUB: {
                    String club = db.getSetting(S_CLUB_TEXT, "Информация о клубе скоро будет обновлена.");
                    sendMenuSection(chatId, club, buildClubMenu());
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_TARIFF: {
                    sendTariffs(chatId, uid, true);
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_REV: {
                    String header = db.getSetting(S_REVIEWS_TEXT, "📝 ОТЗЫВЫ\nЗдесь вы можете посмотреть отзывы:");
                    String url = db.getSetting(S_REVIEWS_URL, "");
                    sendLinkSection(chatId, header, url, true);
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_ABOUT: {
                    String about = db.getSetting(S_ABOUT_TEXT, "Информация «Обо мне» будет обновлена.");
                    sendMenuSection(chatId, about, true);
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_SESS: {
                    String header = db.getSetting(S_SESSIONS_TEXT, "🧘‍♀️ МОИ СЕАНСЫ\nЗдесь Вы можете ознакомиться с моими сеансами:");
                    String url = db.getSetting(S_SESSIONS_URL, "");
                    sendLinkSection(chatId, header, url, true);
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_SUB: {
                    sendSubscriptionStatus(chatId, uid, true);
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_PROCV: {
                    String header = db.getSetting(S_PROCVETA_TEXT, "🌸 КЛУБ «ПРОЦВЕТАЙ»\nОписание будет обновлено.");
                    String url = db.getSetting(S_PROCVETA_URL, "");
                    sendLinkSection(chatId, header, url, true);
                    answerCallback(cb.getId(), "");
                    break;
                }
                case CB_MENU_BACK: {
                    // «Вернуться в начальное меню» — эквивалент /menu
                    String firstName = cb.getFrom().getFirstName() != null ? cb.getFrom().getFirstName() : "друг";
                    sendWelcomeWithMenu(chatId, firstName);
                    answerCallback(cb.getId(), "");
                    break;
                }
                default: answerCallback(cb.getId(), "Неизвестное действие.");
            }
        } catch (Exception e) {
            LOG.error("handleCallback error", e);
        }
    }

    // ===== Разметка и секции =====

    private InlineKeyboardMarkup buildStartGiftKeyboard() {
        String key = db.getSetting(S_GIFT_KEYWORD, "СВОБОДА").trim().toUpperCase(Locale.ROOT);
        InlineKeyboardButton b = new InlineKeyboardButton();
        b.setText("🎁 " + key);
        b.setCallbackData(CB_OPENKW_PREFIX + key);
        InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
        kb.setKeyboard(List.of(List.of(b)));
        return kb;
    }

    private InlineKeyboardMarkup buildIntroKeyboard(Keyword kw) {
        InlineKeyboardButton subscribe = new InlineKeyboardButton();
        subscribe.setText("📢 Подписаться");
        subscribe.setUrl("https://t.me/" + CHANNEL_ID);

        InlineKeyboardButton already = new InlineKeyboardButton();
        already.setText("✅ Уже подписана");
        already.setCallbackData(CB_CHECKSUB_PREFIX + kw.getKeyword());

        InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
        kb.setKeyboard(List.of(List.of(subscribe, already)));
        return kb;
    }

    /** Привет + главное меню (после выдачи бонуса). */
    private void sendWelcomeWithMenu(long chatId, String userName) {
        String raw = db.getSetting(S_WELCOME_TEXT, "{name}, приветствую тебя!\nДобро пожаловать в Soul Way.");
        String text = raw.replace("{name}", userName);

        String videoRef = db.getSetting(S_WELCOME_VIDEO, null);
        InlineKeyboardMarkup menu = buildMainMenu();

        if (videoRef != null && !videoRef.isBlank()) {
            try {
                SendVideo sv = new SendVideo(String.valueOf(chatId), toInputFile(videoRef, true));
                sv.setCaption(text);
                sv.setReplyMarkup(menu);
                execute(sv);
                return;
            } catch (Exception e) { LOG.warn("Не удалось отправить видео приветствия: {}", e.getMessage()); }
        }
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(menu);
        safeExec(sm);
    }

    private InlineKeyboardMarkup buildMainMenu() {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("📘 О КЛУБЕ", CB_MENU_CLUB)));
        rows.add(List.of(btn("💳 ТАРИФЫ", CB_MENU_TARIFF)));
        rows.add(List.of(btn("📝 ОТЗЫВЫ", CB_MENU_REV)));
        rows.add(List.of(btn("👤 ОБО МНЕ", CB_MENU_ABOUT)));
        rows.add(List.of(btn("🧘‍♀️ МОИ СЕАНСЫ", CB_MENU_SESS)));
        rows.add(List.of(btn("🎫 ВАША ПОДПИСКА", CB_MENU_SUB)));
        rows.add(List.of(btn("🌸 КЛУБ «ПРОЦВЕТАЙ»", CB_MENU_PROCV)));
        InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
        kb.setKeyboard(rows);
        return kb;
    }

    private InlineKeyboardMarkup buildClubMenu() {
        InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
        kb.setKeyboard(List.of(
                List.of(btn("💳 ТАРИФЫ", CB_MENU_TARIFF)),
                List.of(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK))
        ));
        return kb;
    }

    private InlineKeyboardButton btn(String text, String data) {
        InlineKeyboardButton b = new InlineKeyboardButton();
        b.setText(text);
        b.setCallbackData(data);
        return b;
    }

    private void sendMenuSection(long chatId, String text, boolean withBack) {
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        if (withBack) {
            InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
            kb.setKeyboard(List.of(List.of(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK))));
            sm.setReplyMarkup(kb);
        }
        safeExec(sm);
    }

    /** Секция с произвольной inline-клавиатурой. */
    private void sendMenuSection(long chatId, String text, InlineKeyboardMarkup kb) {
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(kb);
        safeExec(sm);
    }

    /** Секция с заголовком и внешней ссылкой. */
    private void sendLinkSection(long chatId, String header, String url, boolean withBack) {
        String body = (header == null ? "" : header) + ((url == null || url.isBlank()) ? "" : "\n" + url);
        SendMessage sm = new SendMessage(String.valueOf(chatId), body);
        if (withBack) {
            InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
            kb.setKeyboard(List.of(List.of(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK))));
            sm.setReplyMarkup(kb);
        }
        safeExec(sm);
    }

    // ===== Ссылки оплаты =====

    /** Только значения кодируем, ключи (products[0][name]) оставляем как есть. */
    private static String appendParamsRawKeys(String base, Map<String, String> params) {
        StringBuilder sb = new StringBuilder(base);
        boolean hasQ = base.contains("?");
        for (Map.Entry<String, String> e : params.entrySet()) {
            sb.append(hasQ ? '&' : '?'); hasQ = true;
            sb.append(e.getKey()).append('=').append(urlEncode(e.getValue()));
        }
        return sb.toString();
    }

    /** Строим ссылку оплаты. Работает и с поддоменом, и с поддоменом+рефом. */
    private String buildProdamusLink(String baseUrl, long uid, int days, int priceRub, String productName) {
        String base = (baseUrl == null || baseUrl.isBlank()) ? "https://soulway.payform.ru/" : baseUrl.trim();
        String orderToken = buildOrderIdToken(uid, days); // swb:<uid>:<days>:<hmac>

        Map<String, String> p = new LinkedHashMap<>();
        p.put("do", "pay");
        // На практике Prodamus возвращает наш маркер в order_num — кладём сюда:
        p.put("order_num", orderToken);
        p.put("customer_extra", String.valueOf(uid));
        p.put("products[0][price]", String.valueOf(priceRub));
        p.put("products[0][quantity]", "1");
        p.put("products[0][name]", productName);
        // Дополнительно продублируем суммарную сумму:
        p.put("sum", String.valueOf(priceRub));

        String link = appendParamsRawKeys(base, p);
        LOG.info("[buildProdamusLink] uid={} days={} price={} link={}", uid, days, priceRub, link);
        return link;
    }

    /** ТЕСТ: ссылка на 5 минут (без токена, по названию TEST-5M, обрабатывается в вебхуке). */
    public String buildTest5MinLink(long uid) {
        String base = db.getSetting(S_TAR1_URL, "https://soulway.payform.ru/4e9isVQ/");
        Map<String, String> p = new LinkedHashMap<>();
        p.put("do", "pay");
        p.put("customer_extra", String.valueOf(uid));
        p.put("products[0][price]", "1"); // символическая цена (можно 0, если провайдер позволяет)
        p.put("products[0][quantity]", "1");
        p.put("products[0][name]", "TEST-5M");
        p.put("sum", "1");
        return appendParamsRawKeys(base, p);
    }

    // ИСПРАВЛЕНО: корректно брать цену из конца лейбла («1 МЕС • 1299 ₽» → 1299)
    private static int extractPriceRub(String labelOrPrice, int def) {
        if (labelOrPrice == null) return def;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("([0-9][0-9\\s.,]*)\\s*(?:₽|руб|RUB)?\\s*$", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(labelOrPrice);
        if (!m.find()) return def;
        String num = m.group(1).replaceAll("[\\s.,]", "");
        try { return Integer.parseInt(num); } catch (Exception e) { return def; }
    }

    private void sendTariffs(long chatId, long userId, boolean withBack) {
        String u1 = db.getSetting(S_TAR1_URL, "https://soulway.payform.ru/4e9isVQ/");
        String u2 = db.getSetting(S_TAR2_URL, "https://soulway.payform.ru/en9it1j/");
        String u3 = db.getSetting(S_TAR3_URL, "https://soulway.payform.ru/kr9it4z/");

        String l1 = db.getSetting(S_TAR1_LABEL, "1 МЕС • 1299 ₽");
        int    d1 = safeParseInt(db.getSetting(S_TAR1_DAYS, "30"), 30);
        int    p1 = extractPriceRub(l1, 1299);

        String l2 = db.getSetting(S_TAR2_LABEL, "3 МЕС • 3599 ₽");
        int    d2 = safeParseInt(db.getSetting(S_TAR2_DAYS, "90"), 90);
        int    p2 = extractPriceRub(l2, 3599);

        String l3 = db.getSetting(S_TAR3_LABEL, "12 МЕС • 12900 ₽");
        int    d3 = safeParseInt(db.getSetting(S_TAR3_DAYS, "365"), 365);
        int    p3 = extractPriceRub(l3, 12900);

        String name1 = "Услуги доступа к клубу Путь Души срок 1 месяц";
        String name2 = "Услуги доступа к клубу Путь Души срок 3 месяца";
        String name3 = "Услуги доступа к клубу Путь Души срок 12 месяцев";

        String link1 = buildProdamusLink(u1, userId, d1, p1, name1);
        String link2 = buildProdamusLink(u2, userId, d2, p2, name2);
        String link3 = buildProdamusLink(u3, userId, d3, p3, name3);

        InlineKeyboardButton b1 = new InlineKeyboardButton(l1 + " • оплатить"); b1.setUrl(link1);
        InlineKeyboardButton b2 = new InlineKeyboardButton(l2 + " • оплатить"); b2.setUrl(link2);
        InlineKeyboardButton b3 = new InlineKeyboardButton(l3 + " • оплатить"); b3.setUrl(link3);

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(b1));
        rows.add(List.of(b2));
        rows.add(List.of(b3));
        if (withBack) rows.add(List.of(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK)));

        InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
        kb.setKeyboard(rows);

        String intro = "💳 ТАРИФЫ\n\nВыбери тариф и оплати участие. После оплаты пришлю ссылку в закрытый чат.";
        SendMessage sm = new SendMessage(String.valueOf(chatId), intro);
        sm.setReplyMarkup(kb);
        safeExec(sm);
    }

    private void sendSubscriptionStatus(long chatId, long userId, boolean withBack) {
        Long exp = db.getSubscriptionExpiry(userId);
        String status = (exp == null)
                ? "У вас нет активной подписки."
                : (exp > System.currentTimeMillis() ? "✨ Подписка АКТИВНА до " : "⛔ Подписка истекла ") + df.format(new Date(exp));
        SendMessage sm = new SendMessage(String.valueOf(chatId), "🎫 ВАША ПОДПИСКА\n" + status);
        if (withBack) {
            InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
            kb.setKeyboard(List.of(List.of(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK))));
            sm.setReplyMarkup(kb);
        }
        safeExec(sm);
    }

    // ===== Действия при удачной оплате =====

    public void onProdamusPaid(long uid, int days) {
        try {
            if (uid <= 0) { LOG.warn("onProdamusPaid: пустой uid"); return; }
            db.grantSubscription(uid, days);
            String invite = ensureInviteLink();
            if (invite != null) {
                sendText(uid, "Благодарю за оплату! ✨\nВот ссылка для входа в закрытый чат:\n" + invite);
            } else {
                sendText(uid, "Благодарю за оплату! ✨ Мы скоро пришлём ссылку для входа в чат.");
            }
            // При оплате — отключаем drip-рассылку
            db.deleteDrip(uid);
        } catch (Exception e) { LOG.error("onProdamusPaid error", e); }
    }

    public void onProdamusPaidMinutes(long uid, int minutes) {
        try {
            if (uid <= 0) return;
            db.grantSubscriptionMinutes(uid, minutes);
            String invite = ensureInviteLink();
            if (invite != null) {
                sendText(uid, "Тестовый доступ на " + minutes + " минут выдан. 🔗 Вход в чат:\n" + invite);
            } else {
                sendText(uid, "Тестовый доступ на " + minutes + " минут выдан.");
            }
            db.deleteDrip(uid); // тест доступ — тоже стопаем drip
        } catch (Exception e) { LOG.error("onProdamusPaidMinutes error", e); }
    }

    // ===== Работа с группой =====

    private String ensureInviteLink() {
        String invite = db.getSetting(S_GROUP_INVITE_URL, "");
        if (!invite.isBlank()) return invite;
        String groupIdStr = db.getSetting(S_GROUP_ID, "");
        if (groupIdStr.isBlank()) return null;
        try {
            CreateChatInviteLink req = new CreateChatInviteLink();
            req.setChatId(groupIdStr);
            ChatInviteLink link = execute(req);
            if (link != null && link.getInviteLink() != null) {
                db.setSetting(S_GROUP_INVITE_URL, link.getInviteLink());
                return link.getInviteLink();
            }
        } catch (Exception e) { LOG.warn("Не удалось создать инвайт-ссылку: {}", e.getMessage()); }
        return null;
    }

    private int cleanupExpired() {
        String groupIdStr = db.getSetting(S_GROUP_ID, "");
        if (groupIdStr.isBlank()) return 0;
        int removed = 0;
        List<Long> expired = db.listExpiredSince(System.currentTimeMillis());
        for (Long uid : expired) {
            try {
                BanChatMember ban = new BanChatMember();
                ban.setChatId(groupIdStr);
                ban.setUserId(uid);
                ban.setUntilDate((int) (System.currentTimeMillis() / 1000) + 60);
                execute(ban);
                removed++;
                Thread.sleep(80);
            } catch (Exception e) { LOG.warn("Не удалось удалить {}: {}", uid, e.getMessage()); }
        }
        return removed;
    }

    // ===== Drip-рассылка =====

    private void processDripCampaigns() {
        long now = System.currentTimeMillis();
        List<SQLiteManager.Drip> due = db.listDueDrips(now, 100);
        for (SQLiteManager.Drip d : due) {
            try {
                // Если подписка уже активна — удаляем кампанию и не шлём
                Long exp = db.getSubscriptionExpiry(d.userId);
                if (exp != null && exp > now) {
                    db.deleteDrip(d.userId);
                    continue;
                }

                String text;
                switch (d.step) {
                    case 0: text = DRIP_DAY1; break; // +1 день
                    case 1: text = DRIP_DAY3; break; // +3 день
                    default: text = DRIP_DAY7; break; // +7 день
                }

                SendMessage sm = new SendMessage(String.valueOf(d.userId), text);
                InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
                kb.setKeyboard(List.of(List.of(btn("💳 ТАРИФЫ", CB_MENU_TARIFF))));
                sm.setReplyMarkup(kb);
                safeExec(sm);

                // Переводим на следующий шаг/цикл
                long next;
                int nextStep;
                if (d.step == 0) { // был +1 → следующий через +2 дня (итого 3)
                    next = now + TimeUnit.DAYS.toMillis(2);
                    nextStep = 1;
                } else if (d.step == 1) { // был +3 → следующий через +4 дня (итого 7)
                    next = now + TimeUnit.DAYS.toMillis(4);
                    nextStep = 2;
                } else { // был +7 → с нуля через +1 день
                    next = now + TimeUnit.DAYS.toMillis(1);
                    nextStep = 0;
                }
                db.updateDrip(d.userId, next, nextStep);
                Thread.sleep(80);
            } catch (Exception e) {
                LOG.warn("drip send failed for {}: {}", d.userId, e.getMessage());
            }
        }
    }

    // ===== Награда по ключевому слову =====

    private void sendReward(long chatId, Keyword kw) {
        try {
            List<String> materials = kw.getMaterials();
            String rewardText = safeTrim(kw.getRewardText());

            if (materials == null || materials.isEmpty()) {
                if (!rewardText.isEmpty()) sendText(chatId, rewardText);
                return;
            }

            String image = null;
            List<String> docs = new ArrayList<>();
            for (String m : materials) {
                if (m == null || m.isBlank()) continue;
                String v = m.trim();
                if (isImageSpec(v)) { if (image == null) image = stripImgPrefix(v); else docs.add(v); }
                else docs.add(v);
            }

            if (image != null) sendSinglePhoto(chatId, image, rewardText);
            else if (!rewardText.isEmpty()) sendText(chatId, rewardText);

            if (!docs.isEmpty()) sendDocumentsIndividually(chatId, docs);

        } catch (Exception e) { LOG.error("sendReward error", e); }
    }

    private void sendSinglePhoto(long chatId, String ref, String caption) {
        try {
            SendPhoto sp = new SendPhoto(String.valueOf(chatId), toInputFile(ref, true));
            if (caption != null && !caption.isBlank()) sp.setCaption(caption);
            execute(sp);
        } catch (Exception e) {
            LOG.warn("Фото не отправилось: {}", e.getMessage());
            if (caption != null && !caption.isBlank()) sendText(chatId, caption);
            sendText(chatId, "Материал: " + ref);
        }
    }

    private void sendDocumentsIndividually(long chatId, List<String> docs) {
        for (String ref : docs) {
            try {
                SendDocument sd = new SendDocument(String.valueOf(chatId), toInputFile(ref, true));
                execute(sd);
                Thread.sleep(80);
            } catch (Exception ex2) {
                LOG.warn("Документ '{}' не отправился: {}", ref, ex2.getMessage());
                sendText(chatId, "Материал: " + ref);
            }
        }
    }

    // ===== helpers =====

    private void safeExec(BotApiMethod<?> method) {
        try { execute(method); } catch (Exception e) { LOG.error("execute error", e); }
    }

    private void sendText(long id, String text) {
        try { execute(new SendMessage(String.valueOf(id), text)); }
        catch (Exception e) { LOG.error("sendText error", e); }
    }

    private void answerCallback(String callbackId, String text) {
        try {
            AnswerCallbackQuery ac = new AnswerCallbackQuery();
            ac.setCallbackQueryId(callbackId);
            ac.setText(text);
            ac.setShowAlert(false);
            execute(ac);
        } catch (Exception e) { LOG.error("answerCallback error", e); }
    }

    private static String urlEncode(String s) {
        try { return java.net.URLEncoder.encode(s, "UTF-8"); }
        catch (Exception e) { return s; }
    }

    private static InputFile toInputFile(String ref, boolean tryLocalFirst) {
        String v = safeTrim(ref);
        if (v.isEmpty()) return new InputFile(v);

        if (v.startsWith("files/") || v.startsWith("/files/")) {
            String tail = v.startsWith("/files/") ? v.substring("/files/".length()) : v.substring("files/".length());
            File local = new File(FILES_ROOT + tail);
            return new InputFile(local, local.getName());
        }

        File f = new File(v);
        if (tryLocalFirst && f.exists() && f.isFile()) return new InputFile(f, f.getName());

        if (!v.startsWith("http://") && !v.startsWith("https://") && !v.contains("/")) {
            File local = new File(FILES_ROOT + v);
            if (local.exists() && local.isFile()) return new InputFile(local, local.getName());
        }

        if (v.startsWith("http://") || v.startsWith("https://")) return new InputFile(normalizeUrl(v));
        return new InputFile(v);
    }

    private static String normalizeUrl(String url) {
        if (url == null) return null;
        String v = url.trim();
        if (!(v.startsWith("http://") || v.startsWith("https://"))) return v;
        v = v.replace(" ", "%20").replace("(", "%28").replace(")", "%29")
                .replace("[", "%5B").replace("]", "%5D").replace("{", "%7B").replace("}", "%7D");
        try { return new URI(v).toASCIIString(); } catch (Exception ignore) { return v; }
    }

    private boolean isSubscribedToChannel(long userId) {
        try {
            GetChatMember req = new GetChatMember();
            req.setChatId("@" + CHANNEL_ID);
            req.setUserId(userId);
            ChatMember cm = execute(req);
            if (cm == null) return false;
            String status = cm.getStatus();
            return !"left".equalsIgnoreCase(status) && !"kicked".equalsIgnoreCase(status);
        } catch (Exception e) {
            LOG.warn("getChatMember failed: {}", e.getMessage());
            return false;
        }
    }

    private static String nonEmpty(String s, String def) { return (s == null || s.isBlank()) ? def : s; }
    private static String safeTrim(String s) { return s == null ? "" : s.trim(); }
    private static int safeParseInt(String s, int def) { try { return Integer.parseInt(s.trim()); } catch (Exception e){ return def; } }

    private static boolean isImageSpec(String s) {
        if (s == null) return false;
        String v = s.trim().toLowerCase(Locale.ROOT);
        if (v.startsWith("img:")) return true;
        return v.endsWith(".jpg") || v.endsWith(".jpeg") || v.endsWith(".png")
                || v.endsWith(".webp") || v.endsWith(".heic");
    }
    private static String stripImgPrefix(String s) {
        if (s == null) return null;
        String v = s.trim();
        if (v.toLowerCase(Locale.ROOT).startsWith("img:")) return v.substring(4).trim();
        return v;
    }
}