# HyperAPI
A simple python-powered API for the IUT of Gradignan, developed in cooperation with [Alexandre Boin](https://gitlab.com/alexboin).

## Installation
Install the required packages with [pip](https://pip.pypa.io/en/stable/) :
```bash
$ pip3 install -r requirements.txt
```

## Usage
Use config/calendar.config to set up the classes : 
```
NameOfClass:IdIcal
```
The IdIcal variable can be found in the URL when [exporting the planning to a .ical file](http://www.univ-tln.fr/IMG/pdf/partage-calendrier-synchro-edt.pdf).

Use config/database.config to configure the database path.

### Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
