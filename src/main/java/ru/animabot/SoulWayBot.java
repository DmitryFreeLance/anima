package ru.animabot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.api.methods.BotApiMethod;
import org.telegram.telegrambots.meta.api.methods.groupadministration.BanChatMember;
import org.telegram.telegrambots.meta.api.methods.groupadministration.CreateChatInviteLink;
import org.telegram.telegrambots.meta.api.methods.send.*;
import org.telegram.telegrambots.meta.api.objects.*;
import org.telegram.telegrambots.meta.api.objects.media.InputMedia;
import org.telegram.telegrambots.meta.api.objects.media.InputMediaDocument;
import org.telegram.telegrambots.meta.api.objects.media.InputMediaPhoto;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.meta.api.objects.InputFile;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * SoulWayBot + Prodamus:
 *  - «О клубе» → текст + кнопки «Тарифы», «Назад».
 *  - «Тарифы» → три кнопки-ссылки Prodamus с order_id и days (+ тестовый тариф).
 *  - Webhook Prodamus: активация подписки и приглашение в группу.
 *  - Периодическая чистка истекших подписок.
 */
public class SoulWayBot extends TelegramLongPollingBot {

    private static final Logger LOG = LoggerFactory.getLogger(SoulWayBot.class);

    // ==== Ключи настроек ====
    private static final String S_WELCOME_TEXT = "welcome_text";
    private static final String S_WELCOME_VIDEO = "welcome_video";

    private static final String S_CLUB_TEXT     = "club_text";
    private static final String S_REVIEWS_TEXT  = "reviews_text";
    private static final String S_REVIEWS_URL   = "reviews_url";
    private static final String S_ABOUT_TEXT    = "about_text";
    private static final String S_SESSIONS_TEXT = "sessions_text";
    private static final String S_SESSIONS_URL  = "sessions_url";
    private static final String S_PROCVETA_TEXT = "procveta_text";
    private static final String S_PROCVETA_URL  = "procveta_url";

    // Тарифы (лейблы / дни / ссылки Prodamus)
    private static final String S_TAR1_LABEL = "tariff1_label";
    private static final String S_TAR1_DAYS  = "tariff1_days";
    private static final String S_TAR1_URL   = "tariff1_url";

    private static final String S_TAR2_LABEL = "tariff2_label";
    private static final String S_TAR2_DAYS  = "tariff2_days";
    private static final String S_TAR2_URL   = "tariff2_url";

    private static final String S_TAR3_LABEL = "tariff3_label";
    private static final String S_TAR3_DAYS  = "tariff3_days";
    private static final String S_TAR3_URL   = "tariff3_url";

    // Тестовый тариф (включается, если задан URL)
    private static final String S_TARX_LABEL = "tariffx_label";
    private static final String S_TARX_DAYS  = "tariffx_days";
    private static final String S_TARX_URL   = "tariffx_url";

    private static final String S_GROUP_ID         = "group_id";          // -100...
    private static final String S_GROUP_INVITE_URL = "group_invite_url";  // постоянная ссылка (если задана)

    // Бот / Админ / Канал (лучше хранить только в ENV; дефолты оставлены как было у тебя)
    private final String BOT_TOKEN    = System.getenv().getOrDefault("TG_BOT_TOKEN", "7597890353:AAE8SOaw1tmRBeYkivFCeHoQ1vwI_IWtzMM");
    private final String BOT_USERNAME = System.getenv().getOrDefault("TG_BOT_USERNAME", "SoulWayClub_bot");
    private final long   ADMIN_ID     = Long.parseLong(System.getenv().getOrDefault("TG_ADMIN_ID", "726773708"));
    private final String CHANNEL_ID   = System.getenv().getOrDefault("TG_CHANNEL_ID", "sibirskaiapro"); // без @

    private final SQLiteManager db;
    private final SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");

    // Планировщик авто-очистки истёкших
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public SoulWayBot() {
        db = new SQLiteManager("soulway.db");
        df.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"));
        seedDefaults();
        scheduler.scheduleAtFixedRate(() -> {
            try { cleanupExpired(); } catch (Exception e) { LOG.warn("cleanup error", e); }
        }, 30, 30, TimeUnit.MINUTES);
    }

    private void seedDefaults() {
        putIfEmpty(S_WELCOME_TEXT,
                "Видео\n\n{name}, приветствую тебя!\n" +
                        "Меня зовут Анна Сибирская. \n" +
                        "Приглашаю тебя в закрытый клуб Путь души | Soul Way.\n\n" +
                        "Я создала это пространство для глубокого погружения и слияния со своей Душой, " +
                        "для раскрытия своей силы, потенциалов и новых возможностей ✨\n\n" +
                        "Хочешь почувствовать вкус жизни, энергию, вдохновение? \n\n" +
                        "Присоединяйся ❤️\n\n" +
                        "Посмотреть наполнение клуба можно по кнопке «О Клубе» ниже⤵️\n\n" +
                        "По вопросам: @SoulWayCare.");

        putIfEmpty(S_CLUB_TEXT,
                "📘 О КЛУБЕ\n\n" +
                        "Что ждёт в октябре:\n\n" +
                        "✅ Доступ к записи трех родовых практик для наполнения себя поддержкой Рода\n\n" +
                        "✅ Медитация «Энергетическая защита»\n\n" +
                        "✅ Медитация Состояние Изобилия\n\n" +
                        "✅ 2 онлайн встречи с ченнелинг-медитациями\n\n" +
                        "✅ Курс «Выбери Силу» (18 уроков): как трансформировать негативные состояния и выйти в свою силу\n\n" +
                        "✅ Медитация на гармонизацию негативных состояний\n\n" +
                        "✅ Алгоритм Нейрографики «Работа со страхом»\n\n" +
                        "✅ Общее поле поддержки, чат и общение, возможность задать вопрос\n\n" +
                        "✅ Ежедневные аффирмации\n\n" +
                        "Бонус:\n" +
                        "✅ Только для участниц клуба: скидка 40% на личную сессию\n" +
                        "✅ Медитация — настрой на гармоничный день.");

        putIfEmpty(S_REVIEWS_TEXT,  "📝 ОТЗЫВЫ\nЗдесь вы можете посмотреть отзывы:");
        putIfEmpty(S_REVIEWS_URL,   "https://t.me/sibirskaiapro/336");
        putIfEmpty(S_ABOUT_TEXT,
                "👤 ОБО МНЕ\n\n" +
                        "Меня зовут Анна Сибирская.\n\n" +
                        "✨ Энерготерапевт, энергопрактик, парапсихолог, ченнелер, ведущая трансформационных игр, " +
                        "основательница клубов «Процветай» (https://t.me/procvetaiclub) и «Путь души (Soul Way)». " +
                        "Провожу исцеляющие сеансы. Также художница, пишу энергетические картины 🌸\n\n" +
                        "7 лет назад вела блог о психологии (80k+), потом ушла в глубокую трансформацию, " +
                        "соединение с миссией и Душой. Сейчас — проводник к жизни своей мечты.");
        putIfEmpty(S_SESSIONS_TEXT,"🧘‍♀️ МОИ СЕАНСЫ\nЗдесь Вы можете ознакомиться с моими сеансами:");
        putIfEmpty(S_SESSIONS_URL, "https://t.me/sibirskaiapro/65");
        putIfEmpty(S_PROCVETA_TEXT,"🌸 КЛУБ «ПРОЦВЕТАЙ»\nКлуб с живыми встречами в Санкт-Петербурге ❤️");
        putIfEmpty(S_PROCVETA_URL, "https://t.me/procvetaiclub");

        // Тарифы: текст, дни, ссылки Prodamus
        putIfEmpty(S_TAR1_LABEL, "1 МЕС • 1299 ₽");
        putIfEmpty(S_TAR1_DAYS,  "30");
        putIfEmpty(S_TAR1_URL,   "https://payform.ru/4e9isVQ/");

        putIfEmpty(S_TAR2_LABEL, "3 МЕС • 3599 ₽");
        putIfEmpty(S_TAR2_DAYS,  "90");
        putIfEmpty(S_TAR2_URL,   "https://payform.ru/en9it1j/");

        putIfEmpty(S_TAR3_LABEL, "12 МЕС • 12900 ₽");
        putIfEmpty(S_TAR3_DAYS,  "365");
        putIfEmpty(S_TAR3_URL,   "https://payform.ru/kr9it4z/");

        // Тестовый тариф (можно скрыть, передав пустой URL через /settarifftest)
        putIfEmpty(S_TARX_LABEL, "ТЕСТ • 10 ₽");
        putIfEmpty(S_TARX_DAYS,  "1");
        putIfEmpty(S_TARX_URL,   "https://payform.ru/699mto3/");
    }

    private void putIfEmpty(String key, String value) {
        String cur = db.getSetting(key, null);
        if (cur == null || cur.isBlank()) db.setSetting(key, value);
    }

    @Override public String getBotUsername() { return BOT_USERNAME; }
    @Override public String getBotToken() { return BOT_TOKEN; }

    // ===================== Updates =====================

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

    // ===================== Messages & Commands =====================

    private void handleMessage(Message msg) {
        if (msg == null || msg.getFrom() == null) return;
        long chatId = msg.getChatId();
        String text = msg.hasText() ? msg.getText().trim() : "";

        try {
            if (text.startsWith("/")) {
                // Пытаемся обработать как команду
                boolean handled = handleCommand(msg, text);
                if (handled) return;
                // Если команда неизвестна — считаем, что это кодовое слово со слэшем
                text = text.substring(1).trim();
            }

            if (!text.isEmpty()) {
                Keyword kw = db.findKeywordByKey(text);
                if (kw != null) {
                    SendMessage sm = new SendMessage();
                    sm.setChatId(String.valueOf(chatId));
                    sm.setText(nonEmpty(kw.getIntroText(), "🎁 Подарок:"));
                    sm.setReplyMarkup(buildIntroKeyboard(kw));
                    execute(sm);
                } else {
                    sendText(chatId, "❌ Кодовое слово не найдено. Проверьте написание (без лишнего текста) или обратитесь к администратору.");
                }
            }
        } catch (Exception e) {
            LOG.error("handleMessage error", e);
        }
    }

    /** @return true если команда обработана; false — если неизвестная (пусть станет ключевым словом) */
    private boolean handleCommand(Message msg, String text) {
        long chatId = msg.getChatId();
        long userId = msg.getFrom().getId();
        String[] parts = text.split(" ", 2);
        String cmd = parts[0].toLowerCase(Locale.ROOT);
        String args = parts.length > 1 ? parts[1] : "";

        try {
            switch (cmd) {
                case "/start": {
                    sendText(chatId, "👋 Привет! Это @" + BOT_USERNAME + ".\nВведи кодовое слово из Instagram и получи подарок 🎁");
                    return true;
                }
                case "/addkw": {
                    if (!isAdmin(userId)) { sendText(chatId, "Доступно только администратору."); return true; }
                    if (args.isBlank()) { sendText(chatId, "Формат: /addkw KEY|INTRO|REWARD|materials(через запятую)"); return true; }

                    // Делим на 4 логических поля по первым 3 символам '|'
                    String[] p = args.split("\\|", 4);
                    if (p.length < 3) { sendText(chatId, "Нужно минимум: KEY|INTRO|REWARD"); return true; }

                    String key    = safeTrim(p[0]);
                    String intro  = safeTrim(p[1]);
                    String reward = safeTrim(p[2]);
                    String matsRaw= (p.length == 4) ? safeTrim(p[3]) : "";

                    List<String> mats = new ArrayList<>();
                    if (!matsRaw.isEmpty()) {
                        // Материалы разделяем ТОЛЬКО по запятой; пробелы/скобки в пути сохраняем
                        for (String s : matsRaw.split(",")) {
                            String v = safeTrim(s);
                            if (!v.isEmpty()) mats.add(v);
                        }
                    }

                    Keyword kw = new Keyword(key, intro, reward, mats);
                    db.upsertKeyword(kw);
                    sendText(chatId, "✅ Добавлено/обновлено ключевое слово: " + kw.getKeyword()
                            + (mats.isEmpty() ? "" : "\nМатериалы: " + String.join(", ", mats)));
                    return true;
                }
                case "/listkw": {
                    if (!isAdmin(userId)) { sendText(chatId, "Доступ только администратору."); return true; }
                    List<Keyword> list = db.listKeywords();
                    if (list.isEmpty()) sendText(chatId, "Список пуст.");
                    else {
                        StringBuilder sb = new StringBuilder("Кодовые слова:\n");
                        for (Keyword k : list) sb.append("• ").append(k.getKeyword()).append("\n");
                        sendText(chatId, sb.toString());
                    }
                    return true;
                }

                // ===== редактируемые тексты/ссылки =====
                case "/setwelcome":       if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_WELCOME_TEXT, args); sendText(chatId, "✅ Приветственный текст обновлён."); return true;
                case "/setwelcomevideo":  if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_WELCOME_VIDEO, args); sendText(chatId, "✅ Видео для приветствия обновлено."); return true;
                case "/setclub":          if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_CLUB_TEXT, args);     sendText(chatId, "✅ Текст «О клубе» обновлён."); return true;
                case "/setreviews":       if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_REVIEWS_URL, args);   sendText(chatId, "✅ Ссылка «Отзывы» обновлена."); return true;
                case "/setreviews_text":  if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_REVIEWS_TEXT, args);  sendText(chatId, "✅ Текст «Отзывы» обновлён."); return true;
                case "/setabout":         if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_ABOUT_TEXT, args);    sendText(chatId, "✅ Текст «Обо мне» обновлён."); return true;
                case "/setsessions":      if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_SESSIONS_URL, args);  sendText(chatId, "✅ Ссылка «Мои сеансы» обновлена."); return true;
                case "/setsessions_text": if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_SESSIONS_TEXT, args); sendText(chatId, "✅ Текст «Мои сеансы» обновлён."); return true;
                case "/setprocveta":      if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_PROCVETA_URL, args);  sendText(chatId, "✅ Ссылка «Клуб Процветай» обновлена."); return true;
                case "/setprocveta_text": if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_PROCVETA_TEXT, args); sendText(chatId, "✅ Текст «Клуб Процветай» обновлён."); return true;

                // ===== Тарифы (лейбл/дни/URL) =====
                case "/settariff1": {
                    if (!isAdmin(userId)) { deny(chatId); return true; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) { sendText(chatId, "Формат: /settariff1 LABEL|DAYS|URL"); return true; }
                    db.setSetting(S_TAR1_LABEL, a[0].trim());
                    db.setSetting(S_TAR1_DAYS,  a[1].trim());
                    db.setSetting(S_TAR1_URL,   a[2].trim());
                    sendText(chatId, "✅ Тариф #1 сохранён.");
                    return true;
                }
                case "/settariff2": {
                    if (!isAdmin(userId)) { deny(chatId); return true; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) { sendText(chatId, "Формат: /settariff2 LABEL|DAYS|URL"); return true; }
                    db.setSetting(S_TAR2_LABEL, a[0].trim());
                    db.setSetting(S_TAR2_DAYS,  a[1].trim());
                    db.setSetting(S_TAR2_URL,   a[2].trim());
                    sendText(chatId, "✅ Тариф #2 сохранён.");
                    return true;
                }
                case "/settariff3": {
                    if (!isAdmin(userId)) { deny(chatId); return true; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) { sendText(chatId, "Формат: /settariff3 LABEL|DAYS|URL"); return true; }
                    db.setSetting(S_TAR3_LABEL, a[0].trim());
                    db.setSetting(S_TAR3_DAYS,  a[1].trim());
                    db.setSetting(S_TAR3_URL,   a[2].trim());
                    sendText(chatId, "✅ Тариф #3 сохранён.");
                    return true;
                }
                // ===== Тестовый тариф =====
                case "/settarifftest": {
                    if (!isAdmin(userId)) { deny(chatId); return true; }
                    String[] a = args.split("\\|", 3);
                    if (a.length < 3) {
                        sendText(chatId, "Формат: /settarifftest LABEL|DAYS|URL\nЧтобы скрыть тариф — передай пустой URL (третий параметр).");
                        return true;
                    }
                    db.setSetting(S_TARX_LABEL, a[0].trim());
                    db.setSetting(S_TARX_DAYS,  a[1].trim());
                    db.setSetting(S_TARX_URL,   a[2].trim()); // пустой → кнопка пропадёт
                    sendText(chatId, "✅ Тестовый тариф сохранён.");
                    return true;
                }

                // ===== Группа / Сервисные =====
                case "/setgroup":     if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_GROUP_ID, args.trim());         sendText(chatId, "✅ ID группы сохранён."); return true;
                case "/setgrouplink": if (!isAdmin(userId)) { deny(chatId); return true; } db.setSetting(S_GROUP_INVITE_URL, args.trim()); sendText(chatId, "✅ Инвайт-ссылка группы сохранена."); return true;
                case "/grantsub": {
                    if (!isAdmin(userId)) { deny(chatId); return true; }
                    String[] a = args.split("\\s+");
                    if (a.length < 2) { sendText(chatId, "Формат: /grantsub <userId> <days>"); return true; }
                    long uid = Long.parseLong(a[0]);
                    int days = Integer.parseInt(a[1]);
                    db.grantSubscription(uid, days);
                    sendText(chatId, "✅ Выдана подписка: user " + uid + " на " + days + " дн.");
                    String invite = ensureInviteLink();
                    if (invite != null) sendText(uid, "🔗 Ссылка для входа в чат: " + invite);
                    return true;
                }
                case "/cleanup": {
                    if (!isAdmin(userId)) { deny(chatId); return true; }
                    int n = cleanupExpired();
                    sendText(chatId, "🧹 Удалено из группы: " + n);
                    return true;
                }

                default:
                    // неизвестная команда — пусть обработается как кодовое слово
                    return false;
            }
        } catch (Exception e) {
            LOG.error("handleCommand error", e);
            return true; // считаем обработанной, чтобы не дублировать ошибки в чат
        }
    }

    private boolean isAdmin(long uid) { return uid == ADMIN_ID; }
    private void deny(long chatId) { sendText(chatId, "Доступ только администратору."); }

    // ===================== Callbacks =====================

    private static final String CB_CHECKSUB_PREFIX = "CHECKSUB:";
    private static final String CB_MENU_CLUB   = "MENU:CLUB";
    private static final String CB_MENU_TARIFF = "MENU:TARIFFS";
    private static final String CB_MENU_REV    = "MENU:REVIEWS";
    private static final String CB_MENU_ABOUT  = "MENU:ABOUT";
    private static final String CB_MENU_SESS   = "MENU:SESSIONS";
    private static final String CB_MENU_SUB    = "MENU:SUB";
    private static final String CB_MENU_PROCV  = "MENU:PROCVETA";
    private static final String CB_MENU_BACK   = "MENU:BACK";

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
                answerCallback(cb.getId(), "✅ Подписка подтверждена!");
                // 1) Выдаём бонус (без авто-приветствия внутри)
                sendReward(chatId, kw);
                // 2) ОДИН раз шлём приветствие с меню
                String firstName = cb.getFrom().getFirstName() != null ? cb.getFrom().getFirstName() : "друг";
                sendWelcomeWithMenu(chatId, firstName);
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
                    String firstName = cb.getFrom().getFirstName() != null ? cb.getFrom().getFirstName() : "друг";
                    sendWelcomeWithMenu(chatId, firstName);
                    answerCallback(cb.getId(), "");
                    break;
                }

                default:
                    answerCallback(cb.getId(), "Неизвестное действие.");
            }
        } catch (Exception e) {
            LOG.error("handleCallback error", e);
        }
    }

    // ===================== Витрины и меню =====================

    private InlineKeyboardMarkup buildIntroKeyboard(Keyword kw) {
        InlineKeyboardButton subscribe = new InlineKeyboardButton();
        subscribe.setText("📢 Подписаться");
        subscribe.setUrl("https://t.me/" + CHANNEL_ID);

        InlineKeyboardButton already = new InlineKeyboardButton();
        already.setText("✅ Уже подписана");
        already.setCallbackData(CB_CHECKSUB_PREFIX + kw.getKeyword());

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(Arrays.asList(subscribe, already));

        InlineKeyboardMarkup kb = new InlineKeyboardMarkup();
        kb.setKeyboard(rows);
        return kb;
    }

    /** Привет + меню (видео, если задано). */
    private void sendWelcomeWithMenu(long chatId, String userName) {
        String raw = db.getSetting(S_WELCOME_TEXT, "{name}, приветствую тебя!\nДобро пожаловать в Soul Way.");
        String text = raw.replace("{name}", userName);

        String videoRef = db.getSetting(S_WELCOME_VIDEO, null);
        InlineKeyboardMarkup menu = buildMainMenu();

        if (videoRef != null && !videoRef.isBlank()) {
            try {
                SendVideo sv = new SendVideo();
                sv.setChatId(String.valueOf(chatId));
                File f = new File(videoRef);
                if (f.exists() && f.isFile()) sv.setVideo(new InputFile(f, f.getName()));
                else sv.setVideo(new InputFile(normalizeUrl(videoRef)));
                sv.setCaption(text);
                sv.setReplyMarkup(menu);
                execute(sv);
                return;
            } catch (Exception e) {
                LOG.warn("Не удалось отправить видео приветствия: {}", e.getMessage());
            }
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
        return new InlineKeyboardMarkup(rows);
    }

    private InlineKeyboardMarkup buildClubMenu() {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        rows.add(List.of(btn("💳 ТАРИФЫ", CB_MENU_TARIFF)));
        rows.add(List.of(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK)));
        return new InlineKeyboardMarkup(rows);
    }

    private InlineKeyboardMarkup addBackButton(InlineKeyboardMarkup kb) {
        List<List<InlineKeyboardButton>> rows = (kb == null || kb.getKeyboard() == null)
                ? new ArrayList<>()
                : new ArrayList<>(kb.getKeyboard());
        rows.add(Collections.singletonList(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK)));
        InlineKeyboardMarkup out = new InlineKeyboardMarkup();
        out.setKeyboard(rows);
        return out;
    }

    private void sendMenuSection(long chatId, String text, boolean withBack) {
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(withBack ? addBackButton(null) : null);
        safeExec(sm);
    }

    private void sendMenuSection(long chatId, String text, InlineKeyboardMarkup kb) {
        SendMessage sm = new SendMessage(String.valueOf(chatId), text);
        sm.setReplyMarkup(kb);
        safeExec(sm);
    }

    private void sendLinkSection(long chatId, String header, String url, boolean withBack) {
        String body = header + (url == null || url.isBlank() ? "" : "\n" + url);
        SendMessage sm = new SendMessage(String.valueOf(chatId), body);
        sm.setReplyMarkup(withBack ? addBackButton(null) : null);
        safeExec(sm);
    }

    private void sendTariffs(long chatId, long userId, boolean withBack) {
        // Основные тарифы
        String l1 = db.getSetting(S_TAR1_LABEL, "1 МЕС • 1299 ₽");
        int d1 = safeParseInt(db.getSetting(S_TAR1_DAYS, "30"), 30);
        String u1 = db.getSetting(S_TAR1_URL,  "https://payform.ru/4e9isVQ/");

        String l2 = db.getSetting(S_TAR2_LABEL, "3 МЕС • 3599 ₽");
        int d2 = safeParseInt(db.getSetting(S_TAR2_DAYS, "90"), 90);
        String u2 = db.getSetting(S_TAR2_URL,  "https://payform.ru/en9it1j/");

        String l3 = db.getSetting(S_TAR3_LABEL, "12 МЕС • 12900 ₽");
        int d3 = safeParseInt(db.getSetting(S_TAR3_DAYS, "365"), 365);
        String u3 = db.getSetting(S_TAR3_URL,  "https://payform.ru/kr9it4z/");

        // Тестовый тариф
        String lx = db.getSetting(S_TARX_LABEL, "ТЕСТ • 10 ₽");
        int dx     = safeParseInt(db.getSetting(S_TARX_DAYS, "1"), 1);
        String ux  = db.getSetting(S_TARX_URL,  "");

        // подставляем order_id и days
        String link1 = appendParams(u1, Map.of("order_id", String.valueOf(userId), "days", String.valueOf(d1)));
        String link2 = appendParams(u2, Map.of("order_id", String.valueOf(userId), "days", String.valueOf(d2)));
        String link3 = appendParams(u3, Map.of("order_id", String.valueOf(userId), "days", String.valueOf(d3)));

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();

        // Тестовый тариф — если включён (URL задан), показываем ПЕРВЫМ
        if (ux != null && !ux.isBlank()) {
            String linkX = appendParams(ux, Map.of("order_id", String.valueOf(userId), "days", String.valueOf(dx)));
            InlineKeyboardButton bx = new InlineKeyboardButton(lx + " • оплатить");
            bx.setUrl(linkX);
            rows.add(List.of(bx));
        }

        InlineKeyboardButton b1 = new InlineKeyboardButton(l1 + " • оплатить");
        b1.setUrl(link1);
        InlineKeyboardButton b2 = new InlineKeyboardButton(l2 + " • оплатить");
        b2.setUrl(link2);
        InlineKeyboardButton b3 = new InlineKeyboardButton(l3 + " • оплатить");
        b3.setUrl(link3);

        rows.add(List.of(b1));
        rows.add(List.of(b2));
        rows.add(List.of(b3));
        if (withBack) rows.add(Collections.singletonList(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK)));

        InlineKeyboardMarkup kb = new InlineKeyboardMarkup(rows);

        String intro =
                "💳 ТАРИФЫ\n\n" +
                        "(Имя пользователя), здесь ты можешь выбрать тариф и оплатить участие.\n\n" +
                        "Выбирай тариф и оформляй подписку👇🏻\n\n" +
                        "P.S. Сразу после оплаты я пришлю приглашение в закрытый чат.";

        SendMessage sm = new SendMessage(String.valueOf(chatId), intro);
        sm.setReplyMarkup(kb);
        safeExec(sm);
    }

    private void sendSubscriptionStatus(long chatId, long userId, boolean withBack) {
        Long exp = db.getSubscriptionExpiry(userId);
        String status;
        if (exp == null) {
            status = "У вас нет активной подписки.";
        } else {
            boolean active = exp > System.currentTimeMillis();
            status = (active ? "✨ Подписка АКТИВНА до " : "⛔ Подписка истекла ") + df.format(new Date(exp));
        }

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        if (withBack) rows.add(Collections.singletonList(btn("⬅️ Вернуться в начальное меню", CB_MENU_BACK)));
        InlineKeyboardMarkup kb = new InlineKeyboardMarkup(rows);

        SendMessage sm = new SendMessage(String.valueOf(chatId), "🎫 ВАША ПОДПИСКА\n" + status);
        sm.setReplyMarkup(kb);
        safeExec(sm);
    }

    // ===================== Prodamus: обработка внешнего платежа =====================

    /** Вызывается вебсервером вебхуков после успешной верификации подписи. */
    public void onProdamusPaid(long uid, int days) {
        try {
            db.grantSubscription(uid, days);
            String invite = ensureInviteLink();
            if (invite != null) {
                sendText(uid, "Благодарю за оплату! ✨\nВот ссылка для входа в закрытый чат:\n" + invite);
            } else {
                sendText(uid, "Благодарю за оплату! ✨ Мы скоро пришлём ссылку для входа в чат.");
            }
        } catch (Exception e) {
            LOG.error("onProdamusPaid error", e);
        }
    }

    // ===================== Работа с группой: инвайт и чистка =====================

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
        } catch (Exception e) {
            LOG.warn("Не удалось создать инвайт-ссылку: {}", e.getMessage());
        }
        return null;
    }

    /** Кикаем всех с истёкшей подпиской (бот должен быть админом в группе). */
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
            } catch (Exception e) {
                LOG.warn("Не удалось удалить {}: {}", uid, e.getMessage());
            }
        }
        return removed;
    }

    // ===================== Награда (фото/документы) =====================

    /** ВНИМАНИЕ: здесь НЕТ вызова sendWelcomeWithMenu — приветствие шлётся снаружи один раз. */
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
            if (image != null && docs.isEmpty()) {
                sendSinglePhoto(chatId, image, rewardText);
                return;
            }
            if (image != null) {
                List<InputMedia> firstBatch = new ArrayList<>();
                InputMediaPhoto cover = buildInputMediaPhoto(image, rewardText);
                firstBatch.add(cover);
                sendMediaInBatches(chatId, firstBatch, docs);
            } else {
                if (!rewardText.isEmpty()) sendText(chatId, rewardText);
                sendDocumentsSmart(chatId, docs);
            }
        } catch (Exception e) {
            LOG.error("sendReward error", e);
        }
    }

    private void sendSinglePhoto(long chatId, String ref, String caption) {
        try {
            SendPhoto sp = new SendPhoto(String.valueOf(chatId), toInputFile(ref));
            if (caption != null && !caption.isBlank()) sp.setCaption(caption);
            execute(sp);
        } catch (Exception e) {
            LOG.warn("Фото отдельно не отправилось, шлём ссылкой: {}", e.getMessage());
            if (caption != null && !caption.isBlank()) sendText(chatId, caption);
            sendText(chatId, "Материал: " + ref);
        }
    }

    private void sendDocumentsSmart(long chatId, List<String> docs) {
        if (docs == null || docs.isEmpty()) return;
        if (docs.size() == 1) { sendSingleDocument(chatId, docs.get(0)); }
        else { sendDocumentsAsAlbums(chatId, docs); }
    }

    private void sendDocumentsAsAlbums(long chatId, List<String> docs) {
        int i = 0;
        while (i < docs.size()) {
            int batchSize = Math.min(10, docs.size() - i);
            if (batchSize == 1) { sendSingleDocument(chatId, docs.get(i)); i++; continue; }
            List<InputMedia> pack = new ArrayList<>(batchSize);
            for (int j = 0; j < batchSize; j++) pack.add(buildInputMediaDocument(docs.get(i + j)));
            try {
                SendMediaGroup smg = new SendMediaGroup(String.valueOf(chatId), pack);
                execute(smg);
            } catch (Exception ex) {
                LOG.warn("Альбом документов не отправился, fallback по одному: {}", ex.getMessage());
                for (int j = 0; j < batchSize; j++) sendSingleDocument(chatId, docs.get(i + j));
            }
            i += batchSize;
        }
    }

    private void sendMediaInBatches(long chatId, List<InputMedia> firstBatch, List<String> docs) {
        int idx = 0, room = Math.max(0, 10 - firstBatch.size());
        List<InputMedia> batch = new ArrayList<>(firstBatch);
        while (idx < docs.size() && room-- > 0) batch.add(buildInputMediaDocument(docs.get(idx++)));
        try {
            if (batch.size() < 2) {
                InputMedia first = batch.get(0);
                sendSinglePhoto(chatId, extractMediaRef(first), (first instanceof InputMediaPhoto) ? ((InputMediaPhoto) first).getCaption() : null);
            } else {
                SendMediaGroup smg = new SendMediaGroup(String.valueOf(chatId), batch);
                execute(smg);
            }
        } catch (Exception ex) {
            LOG.warn("Не удалось отправить альбом (с фото), fallback: {}", ex.getMessage());
            InputMedia first = firstBatch.get(0);
            String caption = (first instanceof InputMediaPhoto) ? ((InputMediaPhoto) first).getCaption() : null;
            sendSinglePhoto(chatId, extractMediaRef(first), caption);
            if (idx < docs.size()) sendDocumentsSmart(chatId, docs.subList(idx, docs.size()));
        }
        if (idx < docs.size()) sendDocumentsSmart(chatId, docs.subList(idx, docs.size()));
    }

    private void sendSingleDocument(long chatId, String ref) {
        try {
            SendDocument sd = new SendDocument(String.valueOf(chatId), toInputFile(ref));
            execute(sd);
        } catch (Exception ex2) {
            LOG.warn("Не удалось отправить документ '{}': {}", ref, ex2.getMessage());
            sendText(chatId, "Материал: " + ref);
        }
    }

    private InputMediaPhoto buildInputMediaPhoto(String ref, String caption) {
        InputMediaPhoto photo = new InputMediaPhoto();
        photo.setMedia(ref);
        if (caption != null && !caption.isBlank()) photo.setCaption(caption);
        return photo;
    }

    private InputMediaDocument buildInputMediaDocument(String ref) {
        InputMediaDocument doc = new InputMediaDocument();
        doc.setMedia(ref);
        return doc;
    }

    // ===================== helpers =====================

    private void safeExec(BotApiMethod<?> method) {
        try { execute(method); } catch (Exception e) { LOG.error("execute error", e); }
    }

    private static InlineKeyboardButton btn(String text, String data) {
        InlineKeyboardButton b = new InlineKeyboardButton();
        b.setText(text);
        b.setCallbackData(data);
        return b;
    }

    private void sendText(long chatId, String text) {
        try { execute(new SendMessage(String.valueOf(chatId), text)); }
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

    private static String extractMediaRef(InputMedia im) { return im.getMedia(); }

    private static InputFile toInputFile(String ref) {
        File f = new File(ref);
        if (f.exists() && f.isFile()) return new InputFile(f, f.getName());
        if (ref.startsWith("http://") || ref.startsWith("https://")) return new InputFile(normalizeUrl(ref));
        LOG.warn("toInputFile: локальный файл не найден и это не URL, используем как file_id: {}", ref);
        return new InputFile(ref); // file_id
    }

    private static String appendParams(String base, Map<String,String> q) {
        if (base == null) return null;
        StringBuilder sb = new StringBuilder(base);
        boolean hasQ = base.contains("?");
        for (Map.Entry<String,String> e : q.entrySet()) {
            sb.append(hasQ ? '&' : '?'); hasQ = true;
            sb.append(urlEncode(e.getKey())).append('=').append(urlEncode(e.getValue()));
        }
        return sb.toString();
    }

    private static String urlEncode(String s) {
        try { return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8); }
        catch (Exception e) { return s; }
    }

    private static String normalizeUrl(String url) {
        if (url == null) return null;
        String v = url.trim();
        if (!(v.startsWith("http://") || v.startsWith("https://"))) return v;
        v = v.replace(" ", "%20")
                .replace("(", "%28").replace(")", "%29")
                .replace("[", "%5B").replace("]", "%5D")
                .replace("{", "%7B").replace("}", "%7D");
        try { return new URI(v).toASCIIString(); } catch (Exception ignore) { return v; }
    }

    private static String safeTrim(String s) { return s == null ? "" : s.trim(); }
    private static String nonEmpty(String s, String def) { return (s == null || s.isBlank()) ? def : s; }
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