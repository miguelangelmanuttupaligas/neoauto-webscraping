class Auto:
    def __init__(self, date, price, make, model, body_type, model_year, construction_year, milage, transmition_type, fuel, cylinder, number_of_doors, traction, color, url):
        self._date = date
        self._price = price
        self._make = make
        self._model = model
        self._body_type = body_type
        self._model_year = model_year
        self._construction_year = construction_year
        self._milage = milage
        self._transmition_type = transmition_type
        self._fuel = fuel
        self._cylinder = cylinder
        self._number_of_doors = number_of_doors
        self._traction = traction
        self._color = color
        self._url = url

    @property
    def date(self):
        return self._date