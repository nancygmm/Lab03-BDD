from neo4j import GraphDatabase

class Neo4jMovieGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_node(self, label, properties):
        query = f"""
        CREATE (n:{label} {{ {', '.join([f'{key}: ${key}' for key in properties.keys()])} }})
        RETURN n
        """
        with self.driver.session() as session:
            result = session.run(query, **properties)
            return result.single()["n"]
    
    def create_user(self, user_id, name):
        return self.create_node("User", {"id": user_id, "name": name})

    def create_movie(self, movie_id, title, genre):
        return self.create_node("Movie", {"id": movie_id, "title": title, "genre": genre})
    
    def create_person(self, person_id, name, age):
        return self.create_node("Person", {"id": person_id, "name": name, "age": age})

    def create_rating(self, user_id, movie_id, rating):
        query = """
        MATCH (u:User {id: $user_id}), (m:Movie {id: $movie_id})
        CREATE (u)-[:RATED {score: $rating}]->(m)
        """
        with self.driver.session() as session:
            session.run(query, user_id=user_id, movie_id=movie_id, rating=rating)

    def find_user(self, user_id):
        query = "MATCH (u:User {id: $user_id}) RETURN u"
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id)
            return [record["u"] for record in result]

    def find_movie(self, movie_id):
        query = "MATCH (m:Movie {id: $movie_id}) RETURN m"
        with self.driver.session() as session:
            result = session.run(query, movie_id=movie_id)
            return [record["m"] for record in result]

    def find_user_rating(self, user_id, movie_id):
        query = """
        MATCH (u:User {id: $user_id})-[r:RATED]->(m:Movie {id: $movie_id})
        RETURN u, r, m
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, movie_id=movie_id)
            return [(record["u"], record["r"], record["m"]) for record in result]

# Configurar la conexión a Neo4j AuraDB
URI = "neo4j+s://464fc830.databases.neo4j.io" 
USER = "neo4j"  
PASSWORD = "9KCjnJBJ4_kEXznetBBEr7RhD7UXm9VtP0W_UGsCBk4"  

db = Neo4jMovieGraph(URI, USER, PASSWORD)

# Poblar el grafo con usuarios, películas y personas
db.create_user(1, "Alice")
db.create_user(2, "Bob")
db.create_user(3, "Charlie")
db.create_user(4, "David")
db.create_user(5, "Eve")

db.create_movie(101, "Inception", "Sci-Fi")
db.create_movie(102, "Titanic", "Romance")
db.create_movie(103, "Interstellar", "Sci-Fi")
db.create_movie(104, "The Matrix", "Sci-Fi")
db.create_movie(105, "The Godfather", "Crime")

db.create_person(201, "John Doe", 30)
db.create_person(202, "Jane Smith", 25)

# Crear relaciones de rating
db.create_rating(1, 101, 5)
db.create_rating(1, 102, 4)
db.create_rating(2, 103, 5)
db.create_rating(2, 104, 3)
db.create_rating(3, 105, 4)
db.create_rating(3, 101, 3)
db.create_rating(4, 102, 5)
db.create_rating(4, 103, 4)
db.create_rating(5, 104, 5)
db.create_rating(5, 105, 4)

print("Usuario encontrado:", db.find_user(1))
print("Película encontrada:", db.find_movie(101))
print("Relación de rating encontrada:", db.find_user_rating(1, 101))

db.close()
