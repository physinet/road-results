from database import db


class Model(db.Model):
    row_id = db.Column(db.Integer, primary_key=True)
    racer_id = db.Column(db.Integer)
    place = db.Column(db.Integer)

    def __repr__(self):
        return f"Model: {self.racer_id, self.place}"


def add_sample_rows():
    """Add some sample rows to the database"""
    for racer_id, place in zip([53, 15, 62], [1, 2, 3]):
        row = Model(racer_id=racer_id, place=place)
        db.session.add(row)
        db.session.commit()
