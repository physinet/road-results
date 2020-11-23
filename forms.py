from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField, StringField

from model import Results, Races, Racers


class RaceForm(FlaskForm):
    name_date = StringField('name_date', id='name_date',
                            validators=[Races.validator])
    submit = SubmitField('Show me this race!', id='race_name_submit')

    def __init__(self, race_id, *args, **kwargs):
        """Instantiate the race name selection form. race_id is the id for
        the currently selected race.
        """
        super(RaceForm, self).__init__(*args, **kwargs)

        # Filter to transform empty string into the currently selected race
        self.name_date.filters = [lambda x: x or
                                  Races.get_race_name_date(race_id)]

    def reset_placeholder(self, race_id):
        """Clear form data and set placeholder text to the race name
        corresponding to the given race_id."""
        if self.name_date.data == '':
            if self.name_date.errors:
                self.name_date.errors.pop()  # don't show errors for empty
        self.name_date.description = Races.get_race_name_date(race_id)
        self.name_date.data = ''


class CategoryForm(FlaskForm):
    category = SelectField('Category: ', id='category')
    submit = SubmitField('Show me this category!', id='category_submit')

    def __init__(self, categories, *args, **kwargs):
        super(CategoryForm, self).__init__(*args, **kwargs)
        self.category.choices = categories

class RacerForm(FlaskForm):
    racer_name = StringField('racer_name', id='racer_name',
                             validators=[Racers.validator])
            # Filter to transform empty string into the currently selected racer
    submit = SubmitField('Show me this racer!', id='racer_name_submit')

    def __init__(self, RacerID, *args, **kwargs):
        """Instantiate the racer selection form. RacerID is the id for
        the currently selected racer.
        """
        super(RacerForm, self).__init__(*args, **kwargs)

        # Filter to transform empty string into the currently selected racer
        self.racer_name.filters = [lambda x: x or
                                   Racers.get_racer_name(RacerID)]


    def reset_placeholder(self, RacerID):
        """Clear form data and set placeholder text to the racer name
        corresponding to the given RacerID."""
        if self.racer_name.data == '':
            self.racer_name.errors.pop()  # don't show errors for empty string
        self.racer_name.description = Racers.get_racer_name(RacerID)
        self.racer_name.data = ''
