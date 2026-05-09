from pydoc import text
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import psycopg2
from dotenv import load_dotenv
import os
from datetime import date, timedelta

load_dotenv("/home/clara/TP_PGVD/.env")

app = FastAPI()

# Manejo del error 500, editamos el mensaje.
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "Error interno del servidor. Por favor contacte al administrador."}
    )

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# Endpoint root - Mensaje de bienvenida
@app.get("/")
def root():
    return {"message": "Bienvenido a la app para recomendaciones de productos para los advertisers",
            "Como funciona": "Use el endpoint que desea para obtener distintos resultados",
            "/recommendations/{advertiser_id}/{modelo}":"Para obtener los productos recomendados del dia actual para el advertiser y el modelo elegido.",
            "/history/{advertiser_id}": "Para obtener todas las recomendaciones de producto de los ultimos 7 dias para el advertiser pasado por parametro.",
            "/stats": "Para obtener distintas estadisticas sobre los advertisers y sus productos."}


# Endpoint 1: Recomendaciones

@app.get("/recommendations/{advertiser_id}/{modelo}")
@app.get("/recommendations/{advertiser_id}/")
@app.get("/recommendations/")

def get_recommendations(advertiser_id: str=None, modelo: str=None):
    if advertiser_id is None and modelo is None:
        raise HTTPException(
            status_code = 400,
            detail = "Porfavor completar los parametros con el advertiser y el modelo. Primero va el del advertiser y luego el del modelo."
        )
    if advertiser_id is None:
                raise HTTPException(
            status_code = 400,
            detail = "Porfavor completar todos los parametros, primero el del advetiser y luego el modelo."
        )
    
    if modelo is None:
         raise HTTPException(
        status_code = 400,
        detail = "Por favor completar todos los parámetros, primero el del advertiser y luego el modelo."
    )

    
    conn = get_connection()
    cur = conn.cursor()

    # Verificar si el advertiser existe
    cur.execute("SELECT 1 FROM top_ctr WHERE advertiser_id = %s LIMIT 1", (advertiser_id,))
    if cur.fetchone() is None:
            conn.close()
            raise HTTPException(status_code=400, detail="El advertiser no está activo o no existe.")

    today = date.today()
    modelo_normalizado = modelo.lower().replace("_", "")

    if modelo_normalizado == "topctr":
        cur.execute(
            "SELECT product_id, ctr FROM top_ctr WHERE advertiser_id = %s AND date::date = %s ORDER BY ctr DESC",
            (advertiser_id, today)
        )
    elif modelo_normalizado == "topproducts":
        cur.execute(
            "SELECT product_id, views FROM top_products WHERE advertiser_id = %s AND date::date = %s ORDER BY views DESC",
            (advertiser_id, today)
        )
    else:
        conn.close()
        raise HTTPException(status_code=400, detail="Modelo no válido. Usá TopCTR o TopProducts como modelo.")

    resultados = cur.fetchall()
    conn.close()

    if not resultados:
        raise HTTPException(status_code=404, detail=f"No se encontraron recomendaciones para {advertiser_id} con el modelo {modelo}")

    return {
        "advertiser_id": advertiser_id,
        "modelo": modelo,
        "date": str(today),
        "recommendations": [{"product_id": r[0], "score": r[1]} for r in resultados]
    }
        
# Endpoint 2: Stats

@app.get("/stats/")
def get_stats():
    conn = get_connection()
    cur = conn.cursor()
    today = date.today()

    # Top 10 productos con mayor CTR del día
    cur.execute("""
        SELECT advertiser_id, product_id, ctr 
        FROM top_ctr 
        WHERE date::date = %s::date
        ORDER BY ctr DESC 
        LIMIT 10
    """, (today,))
    top_ctr_results = [{"advertiser_id": r[0], "product_id": r[1], "ctr": r[2]} for r in cur.fetchall()]

    # Coincidencia entre modelos por advertiser, contando cuántos productos en común tienen entre los top 20 de cada modelo
    cur.execute("""
        SELECT t1.advertiser_id, COUNT(*) as productos_en_comun
        FROM top_ctr t1
        JOIN top_products t2 
            ON t1.advertiser_id = t2.advertiser_id 
            AND t1.product_id = t2.product_id 
            AND t1.date::date = t2.date::date
        WHERE t1.date::date = %s::date
        GROUP BY t1.advertiser_id
        ORDER BY productos_en_comun DESC
    """, (today,))
    coincidencia_results = [{"advertiser_id": r[0], "productos_en_comun": r[1]} for r in cur.fetchall()]

    # Total advertisers activos.
    cur.execute("""
        SELECT COUNT(DISTINCT advertiser_id) FROM top_products
    """)
    total_advertisers = cur.fetchone()[0]


    # Producto con mayor CTR por más de un día consecutivo
    cur.execute("""
        SELECT product_id, COUNT(DISTINCT date) as dias_top
        FROM top_ctr
        WHERE ctr = (
            SELECT MAX(ctr) FROM top_ctr t2 
            WHERE t2.date = top_ctr.date
        )
        GROUP BY product_id
        HAVING COUNT(DISTINCT date) > 1
        ORDER BY dias_top DESC
        LIMIT 1
    """)
    producto_con_mayor_ctr_dias_consecutivos = cur.fetchone()
    if producto_con_mayor_ctr_dias_consecutivos:
        mejor_ctr = {"product_id": producto_con_mayor_ctr_dias_consecutivos[0], "cantidad_dias_siendo_top_ctr": producto_con_mayor_ctr_dias_consecutivos[1]}
    else:
        mejor_ctr = {"mensaje": "No existe producto que performe top por más de un día consecutivo"}

    # Producto más visto por más de un día consecutivo
    cur.execute("""
        SELECT product_id, COUNT(DISTINCT date) as dias_top
        FROM top_products
        WHERE views = (
            SELECT MAX(views) FROM top_products t2 
            WHERE t2.date = top_products.date
        )
        GROUP BY product_id
        HAVING COUNT(DISTINCT date) > 1
        ORDER BY dias_top DESC
        LIMIT 1
    """)
    producto_con_mayores_views_dias_consecutivos = cur.fetchone()
    if producto_con_mayores_views_dias_consecutivos:
        mejor_product = {"product_id": producto_con_mayores_views_dias_consecutivos[0], "cantidad_dias_siendo_top_views": producto_con_mayores_views_dias_consecutivos[1]}
    else:
        mejor_product = {"mensaje": "No existe producto que performe top por más de un día consecutivo"}

    conn.close()

    return {
        "date": str(today),
        "top_10_productos_mayor_ctr": top_ctr_results,
        "coincidencia_entre_modelos": coincidencia_results,
        "total_advertisers_activos": total_advertisers,
        "productos_con_mayor_ctr_dias_consecutivos": mejor_ctr,
        "productos_con_mayores_views_dias_consecutivos": mejor_product
    }



# Endpoint 3: Historial

@app.get("/history/{advertiser_id}/")
@app.get("/history/")

def get_history(advertiser_id: str=None):
    if advertiser_id is None:
         raise HTTPException(
            status_code = 400,
            detail = "Porfavor completar con el parametro del Advertiser"
        )

    conn = get_connection()
    cur = conn.cursor()
    today = date.today()
    seven_days_ago = today - timedelta(days=7)

    # Verificar si el advertiser existe
    cur.execute("SELECT 1 FROM top_ctr WHERE advertiser_id = %s LIMIT 1", (advertiser_id,))
    if cur.fetchone() is None:
            conn.close()
            raise HTTPException(status_code=400, detail="El advertiser no está activo o no existe.")

    cur.execute("""
        SELECT product_id, ctr, date 
        FROM top_ctr 
        WHERE advertiser_id = %s AND date::date >= %s::date
        ORDER BY date DESC
    """, (advertiser_id, str(seven_days_ago)))
    top_ctr_history = [{"product_id": r[0], "ctr": r[1], "date": str(r[2])} for r in cur.fetchall()]

    cur.execute("""
        SELECT product_id, views, date 
        FROM top_products 
        WHERE advertiser_id = %s AND date::date >= %s::date
        ORDER BY date DESC
    """, (advertiser_id, str(seven_days_ago)))
    top_products_history = [{"product_id": r[0], "views": r[1], "date": str(r[2])} for r in cur.fetchall()]


    conn.close()
    if not top_ctr_history and not top_products_history:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos históricos para {advertiser_id} en los últimos 7 días. Verifique si el advertiser_id es correcto.")


    return {
        "advertiser_id": advertiser_id,
        "desde": str(seven_days_ago),
        "hasta": str(today),
        "top_ctr_history": top_ctr_history,
        "top_products_history": top_products_history
    }    