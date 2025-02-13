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
        return self.create_node("User", {"name": name, "userId": user_id})

    def create_movie(self, movie_id, title, genre, properties):
        base_properties = {"movieId": movie_id, "title": title, "genre": genre}
        base_properties.update(properties)
        return self.create_node("Movie", base_properties)
    
    def create_person(self, person_id, name, properties):
        base_properties = {"id": person_id, "name": name}
        base_properties.update(properties)
        return self.create_node("Person", base_properties)
    
    def create_genre(self, name):
        return self.create_node("Genre", {"name": name})
    
    def create_relationship(self, node1_label, node1_id, node2_label, node2_id, rel_type, properties={}):
        query = f"""
        MATCH (a:{node1_label} {{id: $node1_id}}), (b:{node2_label} {{id: $node2_id}})
        CREATE (a)-[r:{rel_type} {{ {', '.join([f'{key}: ${key}' for key in properties.keys()])} }}]->(b)
        RETURN r
        """
        with self.driver.session() as session:
            session.run(query, node1_id=node1_id, node2_id=node2_id, **properties)
    
    def find_user(self, user_id):
        query = "MATCH (u:User {userId: $user_id}) RETURN u"
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id)
            return [record["u"] for record in result]

    def find_movie(self, movie_id):
        query = "MATCH (m:Movie {movieId: $movie_id}) RETURN m"
        with self.driver.session() as session:
            result = session.run(query, movie_id=movie_id)
            return [record["m"] for record in result]

URI = "neo4j+s://464fc830.databases.neo4j.io" 
USER = "neo4j"  
PASSWORD = "9KCjnJBJ4_kEXznetBBEr7RhD7UXm9VtP0W_UGsCBk4"  

db = Neo4jMovieGraph(URI, USER, PASSWORD)

db.create_user(1, "Alice")
db.create_user(2, "Bob")
db.create_user(3, "Charlie")
db.create_user(4, "David")
db.create_user(5, "Eve")

db.create_movie(101, "Inception", "Sci-Fi", {"year": 2010, "imdbRating": 8.8})
db.create_movie(102, "Titanic", "Romance", {"year": 1997, "imdbRating": 7.8})

db.create_person(201, "John Doe", {"tmdbId": 12345, "bio": "Famous actor"})
db.create_person(202, "Jane Smith", {"tmdbId": 67890, "bio": "Renowned director"})

db.create_genre("Sci-Fi")
db.create_genre("Romance")

# Crear relaciones
db.create_relationship("User", 1, "Movie", 101, "RATED", {"rating": 5, "timestamp": 16789234})
db.create_relationship("Person", 201, "Movie", 101, "ACTED_IN", {"role": "Lead Actor"})
db.create_relationship("Person", 202, "Movie", 101, "DIRECTED", {"role": "Director"})
db.create_relationship("Movie", 101, "Genre", "Sci-Fi", "IN_GENRE")

print("Usuario encontrado:", db.find_user(1))
print("Película encontrada:", db.find_movie(101))

db.close()