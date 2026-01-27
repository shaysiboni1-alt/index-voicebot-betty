[README.md](https://github.com/user-attachments/files/24887795/README.md)
[README.txt](https://github.com/user-attachments/files/24871420/README.txt)
Index VoiceBot – Betty (SSOT READ ONLY)

מה זה נותן
- טעינת נתונים מהשיטס (SETTINGS / PROMPTS / INTENTS / INTENT_SUGGESTIONS) לקריאה בלבד
- /health מציג ספירות כדי לוודא שהחיבור לשיטס עובד
- /ssot/refresh מאפשר לרענן ידנית

ENV חובה
- GSHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON_B64

בדיקה
1) דיפלוי לרנדר
2) פתח/י:
   https://<YOUR_RENDER_HOST>/health
צפוי לראות settings_keys > 0, prompts_keys > 0, intents > 0
