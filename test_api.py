import os
from dotenv import load_dotenv
import openai

# načteme proměnné z .env
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    print("❌ OPENAI_API_KEY není nastavený. Zkontroluj .env soubor.")
    raise SystemExit

openai.api_key = api_key

print("🔍 Testuji připojení k OpenAI API...")

try:
    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Jsi přátelský asistent."},
            {"role": "user", "content": "Napiš jednou krátkou větou, že AquaCoach API funguje."}
        ],
        max_tokens=50,
        temperature=0.2,
    )
    print("✅ API funguje!")
    print("Odpověď modelu:")
    print(response.choices[0].message.content)

except Exception as e:
    print("❌ Chyba při volání API:")
    print(e)
