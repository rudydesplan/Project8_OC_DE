# transformations.py
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pint import UnitRegistry

ureg = UnitRegistry()
Q_ = ureg.Quantity

# ----------------------------------------------------------
# 1) Direction → degrés
# ----------------------------------------------------------
direction_to_degrees = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
    "North": 0.0, "East": 90.0, "West": 270.0, "South": 180.0
}

def safe_float(value):
    """
    Extrait le premier nombre dans une chaîne, même si le signe est séparé.
    Gère :
        - '12W/m2'
        - '-5.2'
        - '- 5.2'
        - '  -   5.2 lux'
        - '▓13.5'
        - '3e2'
    """
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()

    # regex : signe optionnel + espaces optionnels + nombre
    match = re.search(r"[-+]?\s*\d*[\.,]?\d+(?:[eE][-+]?\d+)?", s)
    if not match:
        return None

    number = match.group().replace(" ", "").replace(",", ".")
    try:
        return float(number)
    except ValueError:
        return None


def safe_int(value):
    """Convertit en int si possible, sinon renvoie None."""
    if value in (None, ""):
        return None
        
    if isinstance(value, int):
        return value

    s = value.strip()
    if s == "":
        return None

    s = re.sub(r"[^0-9\-]", "", s)

    try:
        return int(s)
    except ValueError:
        return None


def safe_float2(value):
    """Convert value to float safely, returns None if empty or invalid."""
    if value in (None, ""):
        return None

    s = value.strip()

    # Empty → None
    if s == "":
        return None

    # Keep only digits, dot, comma, minus
    s = re.sub(r"[^0-9\.,\-]", "", s)

    # Replace comma with dot
    s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None




def safe_okta(value):
    """OKTA = 0 à 8. Chaîne vide => None."""
    if value in ("", None):
        return None
    return safe_int(value)


# ----------------------------------------------------------
# 2) Conversions physiques (Pint)
# ----------------------------------------------------------
def f_to_c(value):
    if value in (None, "", "null"):
        return None
    try:
        return float((Q_(float(value), ureg.degF)).to("degC").magnitude)
    except:
        return None


def mph_to_kmh(value):
    if value in (None, "", "null"):
        return None
    try:
        return float((Q_(float(value), ureg.mile/ureg.hour)).to(ureg.kilometer/ureg.hour).magnitude)
    except:
        return None


def inhg_to_hpa(value):
    if value in (None, "", "null"):
        return None
    try:
        return float((Q_(float(value), ureg.inHg)).to(ureg.hectopascal).magnitude)
    except:
        return None


def inches_to_mm(value):
    if value in (None, "", "null"):
        return None
    try:
        return float((Q_(float(value), ureg.inch)).to(ureg.millimeter).magnitude)
    except:
        return None



# ----------------------------------------------------------
# 3) Direction vent → degré
# ----------------------------------------------------------
def convert_wind_direction(text):
    if not text:
        return None
    return direction_to_degrees.get(text.strip(), None)

# --------------------------------------------------
# Extract date from s3_key (Ichtegem-specific)
# --------------------------------------------------
def extract_date_from_s3_key(s3_key: str):
    """
    Extract DDMMYY only from folders like: /Ichtegem_011024/
    """
    if not s3_key:
        return None

    pattern = r"/(Ichtegem|La_Madeleine)_([0-9]{6})/"
    match = re.search(pattern, s3_key)
    if not match:
        return None

    ddmmyy = match.group(2)

    try:
        return datetime.strptime(ddmmyy, "%d%m%y").date()
    except ValueError:
        return None

# --------------------------------------------------
# Convert local time ("12:04 AM") to UTC using base_date
# --------------------------------------------------
def convert_time_local_to_utc(time_local_str, base_date):
    if not time_local_str or not base_date:
        return None

    # Try multiple time formats
    for fmt in ("%I:%M %p", "%H:%M %p", "%H:%M"):
        try:
            local_t = datetime.strptime(time_local_str, fmt).time()
            break
        except ValueError:
            local_t = None

    if not local_t:
        return None

    # Local time is Europe/Paris
    dt_local = datetime.combine(base_date, local_t).replace(
        tzinfo=ZoneInfo("Europe/Paris")
    )

    return dt_local.astimezone(ZoneInfo("UTC"))


def transform_infoclimat(doc):
    """
    Transforme un document InfoClimat vers le schéma final hourly_measurements.
    Toutes les valeurs sont en string → converties safe_float ou safe_int.
    """

    return {
        "id_station": doc.get("id_station"),

        # InfoClimat fournit dh_utc directement comme datetime dans staging.
        "dh_utc": doc.get("dh_utc"),
        "s3_key": doc.get("s3_key"),

        # Champs météo principaux
        "temperature_C": safe_float2(doc.get("temperature_C")),
        "pression_hPa": safe_float2(doc.get("pression_hPa")),
        "humidite_pct": safe_float2(doc.get("humidite_pct")),
        "point_de_rosee_C": safe_float2(doc.get("point_de_rosee_C")),
        "visibilite_m": safe_float2(doc.get("visibilite_m")),

        # Vent
        "vent_moyen_kmh": safe_float2(doc.get("vent_moyen_kmh")),
        "vent_rafales_kmh": safe_float2(doc.get("vent_rafales_kmh")),
        "vent_direction_deg": safe_float2(doc.get("vent_direction_deg")),

        # Précipitations
        "pluie_3h_mm": safe_float2(doc.get("pluie_3h_mm")),
        "pluie_1h_mm": safe_float2(doc.get("pluie_1h_mm")),
        "neige_au_sol_cm": safe_float2(doc.get("neige_au_sol_cm")),

        # Ciel
        "nebulosite_okta": safe_int(doc.get("nebulosite_okta")),
        "temps_omm_code": safe_int(doc.get("temps_omm_code")),

        # Champs optionnels (peuvent exister dans staging)
        "uv_index": safe_float2(doc.get("uv_index")),
        "solar_wm2": safe_float2(doc.get("solar_wm2")),
    }


# ----------------------------------------------------------
# 5) Transformation du document Ichtegem / Madeleine
# ----------------------------------------------------------
def transform_document(doc):

    s3_key = doc.get("s3_key")
    extracted_date = extract_date_from_s3_key(s3_key)

    return {
        "id_station": doc.get("id_station"),
        "s3_key": doc.get("s3_key"),

        # ---- DATE UTC ----
        "dh_utc": convert_time_local_to_utc(
            doc.get("time_local"),
            base_date=extracted_date
        ),

        # ---- TEMPÉRATURE & HUMIDITÉ ----
        "temperature_C": f_to_c(safe_float2(doc.get("temperature_F"))),
        "point_de_rosee_C": f_to_c(safe_float2(doc.get("dew_point_F"))),
        "humidite_pct": safe_float2(doc.get("humidity_pct")),

        # ---- VENT ----
        "vent_moyen_kmh": mph_to_kmh(safe_float2(doc.get("wind_speed_mph"))),
        "vent_rafales_kmh": mph_to_kmh(safe_float2(doc.get("wind_gust_mph"))),
        "vent_direction_deg": convert_wind_direction(safe_float2(doc.get("wind_direction_text"))),

        # ---- PRESSION ----
        "pression_hPa": inhg_to_hpa(safe_float2(doc.get("pressure_inHg"))),

        # ---- PRÉCIPITATIONS via Pint ----
        "precip_rate_mm": inches_to_mm(safe_float2(doc.get("precip_rate_in"))),
        "precip_accum_mm": inches_to_mm(safe_float2(doc.get("precip_accum_in"))),

        # ---- RAYONNEMENT ----
        "solar_wm2": safe_float2(doc.get("solar_wm2")),
        "uv_index": safe_float2(doc.get("uv_index")),
    }
