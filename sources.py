"""מקורות ברירת מחדל למערכת הניטור.

הערה: כתובות RSS משתנות מדי פעם. המערכת מריצה בדיקת תקינות בעלייה
(validate_rss_feeds) ומדפיסה ללוג אילו פידים לא ענו, כדי שלא ייווצר מצב
של מקור "מת" בשקט.
"""

# ---------------------------------------------------------------------------
# ערוצי טלגרם ציבוריים (נקראים דרך t.me/s/<channel>)
# ---------------------------------------------------------------------------

# ערוצים ראשוניים — הנושא עצמו מפרסם בהם.
# כל פוסט מהם רלוונטי בהגדרה, גם בלי שם המטרה בטקסט, ונשלח תמיד
# גם אם התקשורת כבר דיווחה על התוכן.
PRIMARY_TELEGRAM = {
    "bnetanyahu": "netanyahu",
}

# ערוצי פרשנים — הניסוח שלהם הוא הידיעה, לכן כל אזכור מהם נשלח
# בנפרד גם אם הסיפור כבר דווח במקום אחר (עוקף דדופ, אבל עדיין
# חייב לעבור את פילטר הרלוונטיות).
ALWAYS_ALERT_TELEGRAM = {"amitsegal", "lieldaphna"}

TELEGRAM_CHANNELS = {
    # ערוץ רשמי של הנושא
    "bnetanyahu": "📢 נתניהו — ערוץ רשמי",

    # פרשנים וכתבים פוליטיים
    "amitsegal": "🎙️ עמית סגל",
    "lieldaphna": "🎙️ דפנה ליאל",
    "MichaelShemesh": "🎙️ מיכאל שמש",
    "no_politix": "🎙️ אטינגר ושגב",
    "yaronyanir1299": "🎙️ ירון אברהם ויניר קוזין",
    "Political_arena": "🎙️ זירה פוליטית",

    # טלוויזיה
    "N12_news_Israel": "📺 חדשות 12",
    "N12nws": "📺 N12 מבזקים",
    "kann_news": "📺 כאן חדשות",
    "KanNewsTwitter": "📺 כאן — ציוצים",
    "newsisrael13": "📺 חדשות 13",
    "Now14Israel": "📺 עכשיו 14",
    "Channel14_News": "📺 ערוץ 14 — חדשות",
    # עיתונות
    "ynet": "📰 Ynet",
    "WallaNews": "📰 וואלה",
    "MaarivOnline": "📰 מעריב",
    "yediotnews25": "📰 ידיעות אחרונות",
    "IsraelHayomNews": "📰 ישראל היום",
    "Haaretz": "📰 הארץ",
    # מבזקים ובטחון
    "abualiexpress": "⚡ אבו עלי",
    "HamalNews": "⚡ החמ״ל",
    "hamoked_il": "⚡ המוקד",
    "yinonews": "⚡ ינון ניוז",
    "israel_yamin": "⚡ ישראל ימין",
    "NewsFromTheField": "⚡ חדשות מהשטח",
    "RoterNews": "⚡ רוטר",
    "MivzakLive": "⚡ מבזקים",
    "RealTimeSecurity": "⚡ ביטחון שוטף",
    "DoverTzahal": "⚡ דובר צה״ל",
    "almog_cohen_news": "⚡ אלמוג כהן",
    "PushNews": "⚡ פוש ניוז",
    "BreakingNewsIL": "⚡ מבזקים IL",
    # רדיו (ערוצי טקסט — לא תמלול שידור)
    "GLZRadio": "📻 גל״צ",
    "israel1": "📻 רשת ב׳ / כאן",
    "GaleyTzahal": "📻 גלי צה״ל",
    "galeyisrael": "📻 גלי ישראל",
    # סקרים
    "PollsIsrael": "📊 סקרים",
}

# ---------------------------------------------------------------------------
# פידי RSS
# ---------------------------------------------------------------------------

# פידים שאומתו כעובדים מהשרת ב-Render (בדיקה: 29.8.2026).
# לבדיקת כתובת חדשה: /testfeed?url=...  להוספה בלי דיפלוי: /addfeed
RSS_FEEDS = {
    "Ynet": "https://www.ynet.co.il/Integration/StoryRss2.xml",
    "Ynet פוליטי": "https://www.ynet.co.il/Integration/StoryRss1854.xml",
    "וואלה": "https://rss.walla.co.il/feed/22",
    "וואלה פוליטי": "https://rss.walla.co.il/feed/2686",
    "מאקו — חדשות 12": "https://rcs.mako.co.il/rss/news-military.xml",
    "הארץ": "https://www.haaretz.co.il/srv/htz---all-articles",
    "כיכר השבת": "https://www.kikar.co.il/feed",
    "ערוץ 7": "https://www.inn.co.il/Rss.aspx",
    "גלובס": "https://www.globes.co.il/webservice/rss/rssfeeder.asmx/FeederNode?iID=1725",
    "סרוגים": "https://www.srugim.co.il/feed",
    "JPost": "https://www.jpost.com/rss/rssfeedsisraelnews.aspx",
    "🌍 סיקור זר — נתניהו": "https://news.google.com/rss/search?q=Netanyahu&hl=en-US&gl=US&ceid=US:en",
}

# נבדקו ונפסלו (403 חסימת IP או 404): ישראל היום, מעריב, כאן,
# חדשות 13, כלכליסט, בחדרי חרדים, ביזפורטל, זמן ישראל, TheMarker,
# Times of Israel, i24. רובם מכוסים בערוצי הטלגרם המקבילים.

# ---------------------------------------------------------------------------
# משקלי מקורות — לא כל מקור שווה. משמש לדירוג משמעותיות ולזיהוי
# מי באמת פרסם ראשון (אגרגטור שהעתיק מ-Ynet אינו פורץ ידיעה).
# ---------------------------------------------------------------------------

DEFAULT_SOURCE_WEIGHT = 0.6

SOURCE_WEIGHTS = {
    # מקור ראשוני
    "bnetanyahu": 1.0,
    # פרשנים בכירים
    "amitsegal": 1.0,
    "lieldaphna": 0.95,
    "MichaelShemesh": 0.95,
    "no_politix": 0.9,
    "yaronyanir1299": 0.9,
    "Political_arena": 0.7,
    # חדשות מובילות
    "N12_news_Israel": 1.0,
    "N12nws": 0.95,
    "KanNewsTwitter": 0.9,
    "hamoked_il": 0.6,
    "yinonews": 0.6,
    "israel_yamin": 0.55,
    "kann_news": 1.0,
    "newsisrael13": 0.95,
    "Now14Israel": 0.9,
    "Channel14_News": 0.9,
    "ynet": 1.0,
    "Haaretz": 0.95,
    "WallaNews": 0.85,
    "IsraelHayomNews": 0.85,
    "MaarivOnline": 0.8,
    "PollsIsrael": 0.9,
    "DoverTzahal": 0.9,
    "galeyisrael": 0.8,
    "GLZRadio": 0.8,
    "israel1": 0.85,
    "yediotnews25": 0.85,
    "GaleyTzahal": 0.85,
    "abualiexpress": 0.75,
    # מבזקים ואגרגטורים — מהירים אך לרוב מעתיקים
    "HamalNews": 0.6,
    "NewsFromTheField": 0.55,
    "RealTimeSecurity": 0.55,
    "almog_cohen_news": 0.55,
    "RoterNews": 0.5,
    "MivzakLive": 0.45,
    "BreakingNewsIL": 0.45,
    "PushNews": 0.45,
    # פידי RSS (המפתח הוא שם הפיד)
    "Ynet": 1.0,
    "וואלה": 0.85,
    "ישראל היום": 0.85,
    "מעריב": 0.8,
    "ערוץ 7": 0.7,
    "גלובס": 0.75,
    "סרוגים": 0.65,
    "JPost": 0.8,
    "Times of Israel": 0.8,
    "Ynet פוליטי": 1.0,
    "וואלה פוליטי": 0.85,
    "מאקו — חדשות 12": 1.0,
    "הארץ": 0.95,
    "כיכר השבת": 0.7,
    "🌍 סיקור זר — נתניהו": 0.9,
}
