import asyncio
import pandas as pd
from datetime import datetime
from telegram import Bot
from sqlalchemy import create_engine
from config import DB_CONFIG, TELEGRAM_TOKEN, CHAT_ID
from sqlalchemy import text


print("✅ Librerías funcionando, bot listo.")

# Crear conexión SQLAlchemy (sin warnings)
DB_URL = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URL)

bot = Bot(token=TELEGRAM_TOKEN)

def obtener_datos():
    """Obtiene las incidencias con riesgo ALTO que no fueron enviadas"""
    query = "SELECT * FROM incidentes WHERE nivel_riesgo = 'ALTO' AND enviado = 0;"
    df = pd.read_sql(query, engine)
    return df

async def enviar_alerta(row):
    """Envía una alerta al chat de Telegram"""
    mensaje = (
        f"🚨 *ALERTA DE PELIGRO*\n\n"
        f"📍 Zona: {row['zona']}\n"
        f"🧾 Descripción: {row['descripcion']}\n"
        f"⚠️ Nivel: {row['nivel_riesgo']}\n"
        f"🕐 {row['fecha']}"
    )
    await bot.send_message(chat_id=CHAT_ID, text=mensaje, parse_mode="Markdown")

def actualizar_envio(id_incidencia):
    """Marca la alerta como enviada en la base de datos"""
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE incidentes SET enviado = 1 WHERE id = :id"),
            {"id": id_incidencia}
        )


async def main():
    incidentes = obtener_datos()
    if not incidentes.empty:
        tareas = []
        for _, row in incidentes.iterrows():
            tareas.append(enviar_alerta(row))
            actualizar_envio(row["id"])
        await asyncio.gather(*tareas)
        print(f"✅ {len(incidentes)} alertas enviadas correctamente.")
    else:
        print("📭 No hay alertas nuevas para enviar.")

if __name__ == "__main__":
    asyncio.run(main())
