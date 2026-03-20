

# --------------------- Imports ---------------------
import csv
import random
from configs.config import BASE_DIR, TEXT_DATA_PATH, VITALS_DATA_PATH, IMAGES_DIR

# --------------------- Paths ---------------------
IMAGE_DIR = BASE_DIR / "dummydata" / "images"
OUTPUT_DIR = BASE_DIR / "dummydata"

TEXT_FILE = OUTPUT_DIR / "text.csv"
VITALS_FILE = OUTPUT_DIR / "vitals.csv"


BODY_PARTS = ["hand", "wrist", "ankle", "leg", "hip", "pelvis", "chest"]
GENDERS = ["Male", "Female"]

def generate_report(body_part):
    templates = [
        f"X-ray shows a fracture in the {body_part}. Mild displacement observed.",
        f"Fracture detected in the {body_part}. Alignment is compromised.",
        f"Clear fracture line visible in the {body_part}. Recommend immobilization.",
        f"{body_part.capitalize()} fracture with soft tissue swelling.",
    ]
    return random.choice(templates)

def generate_vitals():
    return {
        "age": random.randint(18, 80),
        "gender": random.choice(GENDERS),
        "heart_rate": random.randint(80, 110),  
        "bp_systolic": random.randint(110, 150),
        "bp_diastolic": random.randint(70, 95),
        "temperature": round(random.uniform(36.5, 37.5), 1),
    }

def main():

    images = list(IMAGE_DIR.glob("*.jpg"))

    with open(TEXT_FILE, "w", newline="") as tf, open(VITALS_FILE, "w", newline="") as vf:
        
        text_writer = csv.writer(tf)
        vitals_writer = csv.writer(vf)

        # Headers
        text_writer.writerow(["sample_id", "image_path", "body_part", "diagnosis", "report"])
        vitals_writer.writerow(["sample_id", "age", "gender", "heart_rate", "bp_systolic", "bp_diastolic", "temperature"])

        for idx, img_path in enumerate(images):
            sample_id = f"sample_{idx}"

            body_part = random.choice(BODY_PARTS)
            diagnosis = "fracture"
            report = generate_report(body_part)

            # text.csv
            text_writer.writerow([
                sample_id,
                str(img_path),
                body_part,
                diagnosis,
                report
            ])

            vitals = generate_vitals()

            # vitals.csv
            vitals_writer.writerow([
                sample_id,
                vitals["age"],
                vitals["gender"],
                vitals["heart_rate"],
                vitals["bp_systolic"],
                vitals["bp_diastolic"],
                vitals["temperature"]
            ])

    print("Generated text.csv and vitals.csv successfully.")

if __name__ == "__main__":
    main()