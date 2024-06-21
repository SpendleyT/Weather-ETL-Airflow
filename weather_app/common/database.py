from sqlalchemy import create_engine, func, select, update, delete, text
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey
from sqlalchemy.orm import Session, declarative_base
import psycopg2
from pandas import Series
import os

#Define ORM data class base
Base = declarative_base()

#Define database configurations
DB_NAME = 'city_weather_db'
DB_ADDRESS = 'localhost:5432'
DB_USER = 'postgres'
DB_PASSWORD = os.environ['DB_PASSWORD_KEY']


class DatabaseConnection:
    """ Class to manage database transactions"""
    def __init__(self):
        self._engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_ADDRESS}/{DB_NAME}",
            isolation_level = "REPEATABLE READ"
        )
        self._session = Session(self._engine)


    def close(self):
        """ Ends connections with the database"""
        self._session.close_all()
        self._engine.dispose()
        return True


    def add_weather_entry(self, weahter_info: Series, city_id: int):
        """
        Adds a new row to the box_office table for a specific movie/week combination.

        :params week_info: box office data to be stored
        :params movie_id: foreign key for the previously stored movie
        """
        weather_info = WeatherInfo(

        )
        self._session.add(weather_info)
        self._session.flush()
        id = weather_info.box_office_id
        self._session.commit()
        return True


    def add_city(self, city_info: Series):
        """
        Adds a new movie record.

        :params movie_info: row data to get the movie dimension data
        :params dist_id: foreign key for the distributor listed
        """
        city_info = WeatherInfo(
            
        )
        self._session.add(new_movie)
        self._session.flush()
        id = new_movie.movie_id
        self._session.commit()
        return id


    # def add_distributor(self, distributor: str) -> int:
    #     """ 
    #     Stores distributor info for reference
        
    #     :params distributor: name of the distributor that is to be stored
    #     """
    #     new_distributor = Distributor(distributor_name=distributor)
    #     self._session.add(new_distributor)
    #     self._session.flush()
    #     id = new_distributor.distributor_id
    #     self._session.commit()
    #     return id


    def get_city_id_by_name(self, city: str):
        """ 
        Retrieves the movie id for reference.

        :params title: movie title for retrieval of existing record id

        :returns movie_id: if exists, 0 if not
        """
        stmt = select(Movie).where(Movie.title == title)
        result = self._session.execute(stmt).fetchone()
        return 0 if not result else result[0].movie_id


    # def get_distributor_id_by_name(self, name: str):
    #     """
    #     :params name: distributor name for retrieval of existing record id

    #     :returns distributor_id: if exists, 0 if not
    #     """
    #     stmt = select(Distributor).where(Distributor.distributor_name == name)
    #     result = self._session.execute(stmt).fetchone()
    #     return 0 if not result else result[0].distributor_id



class Distributor(Base):
    """ ORM class for the distributor info"""
    __tablename__ = "distributor"
    distributor_id = Column(Integer, primary_key=True)
    distributor_name = Column(String)


class Movie(Base):
    """ ORM class for the movie info """
    __tablename__ = "movie"
    movie_id = Column(Integer, primary_key=True)
    distributor_id = Column(
        Integer, 
        ForeignKey('distributor.distributor_id', ondelete='CASCADE')
    )
    title = Column(String)
    release_year = Column(Integer)
    runtime = Column(Integer)
    poster_url = Column(String)
    box_office_gross = Column(Integer)
    meta_score = Column(Float)
    rt_score = Column(Float)
    imdb_score = Column(Float)
    imdb_ref_id = Column(String)
    genre = Column(String)


class BoxOffice(Base):
    """ ORM class for box office info """
    __tablename__  = 'box_office'
    box_office_id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movie.movie_id', ondelete='CASCADE'))
    bo_year = Column(Integer)
    bo_week = Column(Integer)
    bo_rank = Column(Integer)
    weekly_gross = Column(Integer)
    theatres = Column(Integer)
    per_theatre = Column(Integer)
    release_week = Column(Integer)
