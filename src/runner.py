from src.mongo_client import mongoDbManager
from src.requests import Requests


class Runner:

    def __init__(self):
        self.mongo = mongoDbManager()
        self.collection = self.mongo.get_collection()
        self.requests = Requests()

    def run_pipeline(self):
        # Lit la collection et transforme en un Dataframe
        df = self.requests.read_db(self.collection)

        # Calculer le taux de réservation moyen par mois par type de logement
        print("1️⃣ Taux de réservation par type de logement ✅")
        print(self.requests.mean_reservation(df, "availability_365", "property_type"))

        # Calculer la médiane des nombre d’avis pour tous les logements
        print("2️⃣ Nombre médian d'avis ✅")
        print(self.requests.describe_review(df, "number_of_reviews"))

        # Calculer la médiane des nombre d’avis par catégorie d’hôte
        print("3️⃣ Nombre médian d'avis par catégorie d'hôte ✅")
        print(
            self.requests.median_review_per_host_type(
                df, "number_of_reviews", "host_is_superhost"
            )
        )

        # Calculer la densité de logements par quartier de Paris
        print("4️⃣ Densité de logement par quartier ✅")
        print(self.requests.density(df, "host_neighbourhood"))

        # Identifier les quartiers avec le plus fort taux de réservation par mois
        print("5️⃣ Quartier avec le + fort taux de réservation ✅")
        print(
            self.requests.reservation_rate(df, "availability_365", "host_neighbourhood")
        )


def main():
    runner = Runner()
    runner.run_pipeline()


if __name__ == "__main__":
    main()
