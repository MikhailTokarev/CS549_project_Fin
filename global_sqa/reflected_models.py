## reflected_models.py ##
"""automaps movies table to SQA Base class
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqa_config import DATABASE_URI

import sys, os

engine = create_engine(DATABASE_URI, echo=True)
Base = declarative_base(engine)

class Global(Base):
    __tablename__ = 'global'
    __table_args__ = {'autoload': True}

    def __repr__(self):
        return ">Title:{title} \tYear: {year} \tId: {id} \tDup: {dup}" \
               "\nDirector:{director}"\
               "\nCast:{casts}"\
               "\nCountry:{country} \tGenre: {genre}"\
               "\nDescription: {description}"\
                .format(id = self.id,
                        title = self.title,
                        year = self.year,
                        dup = self.duplicate,
                        genre = self.genre,
                        director = self.director,
                        country = self.country,
                        casts = self.casts,
                        description = self.description)

if __name__ == "__main__":
    Session = sessionmaker(bind=engine)
    session = Session()
    res = session.query(MyTable).all()
    print(res[1].movie_name)