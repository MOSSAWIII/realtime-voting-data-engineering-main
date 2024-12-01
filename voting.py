import random
import time
from datetime import datetime
import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

def process_initial_voters(cur, conn, candidates):
    """Traite les électeurs existants qui n'ont pas encore voté"""
    cur.execute("""
        SELECT v.voter_id, v.voter_name, v.date_of_birth, v.gender, v.nationality, 
               v.registration_number, v.address_street, v.address_city, v.address_state, 
               v.address_country, v.address_postcode, v.email, v.phone_number, 
               v.cell_number, v.picture, v.registered_age
        FROM voters v
        LEFT JOIN votes vt ON v.voter_id = vt.voter_id
        WHERE vt.voter_id IS NULL
    """)
    unvoted_voters = cur.fetchall()
    print(f"Found {len(unvoted_voters)} voters who haven't voted yet")

    for voter in unvoted_voters:
        process_vote(create_voter_data(voter), candidates, cur, conn)

def create_voter_data(voter_tuple):
    """Crée un dictionnaire d'électeur à partir d'un tuple de base de données"""
    return {
        "voter_id": voter_tuple[0],
        "voter_name": voter_tuple[1],
        "date_of_birth": voter_tuple[2],
        "gender": voter_tuple[3],
        "nationality": voter_tuple[4],
        "registration_number": voter_tuple[5],
        "address": {
            "street": voter_tuple[6],
            "city": voter_tuple[7],
            "state": voter_tuple[8],
            "country": voter_tuple[9],
            "postcode": voter_tuple[10]
        },
        "email": voter_tuple[11],
        "phone_number": voter_tuple[12],
        "cell_number": voter_tuple[13],
        "picture": voter_tuple[14],
        "registered_age": voter_tuple[15]
    }

def process_vote(voter, candidates, cur, conn):
    """Traite un vote pour un électeur"""
    try:
        # Vérifier si l'électeur a déjà voté
        cur.execute("SELECT voter_id FROM votes WHERE voter_id = %s", (voter['voter_id'],))
        if cur.fetchone() is not None:
            print(f"Voter {voter['voter_id']} has already voted, skipping...")
            return

        # Créer et enregistrer le vote
        chosen_candidate = random.choice(candidates)
        vote = voter | chosen_candidate | {
            "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "vote": 1
        }

        cur.execute("""
            INSERT INTO votes (voter_id, candidate_id, voting_time)
            VALUES (%(voter_id)s, %(candidate_id)s, %(voting_time)s)
        """, vote)
        conn.commit()

        producer.produce(
            'votes_topic',
            key=vote["voter_id"],
            value=json.dumps(vote),
            on_delivery=delivery_report
        )
        producer.poll(0)
        print(f"User {vote['voter_id']} voted for {vote['candidate_name']}")

    except Exception as e:
        print(f"Error processing vote: {e}")
        conn.rollback()

if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Récupérer les candidats
    cur.execute("""
        SELECT row_to_json(t)
        FROM (SELECT * FROM candidates) t;
    """)
    candidates = [candidate[0] for candidate in cur.fetchall()]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")

    # D'abord traiter les électeurs existants qui n'ont pas voté
    process_initial_voters(cur, conn, candidates)

    # Ensuite commencer à écouter les nouveaux électeurs
    consumer.subscribe(['voters_topic'])
    print("Starting to listen for new voters...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                process_vote(voter, candidates, cur, conn)
                time.sleep(0.05)  # Petit délai pour ne pas surcharger

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()
        conn.close()