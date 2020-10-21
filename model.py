from database import db


class Model(db.Model):
    row_id = db.Column(db.Integer, primary_key=True)
    racer_id = db.Column(db.Integer)
    place = db.Column(db.Integer)

    def __repr__(self):
        return f"Model: {self.racer_id, self.place}"
