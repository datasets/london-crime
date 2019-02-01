This dataset was scraped from [London data](https://data.london.gov.uk/) website.

Numbers of recorded offences, and rates of offences per thousand population, by broad crime grouping, by financial year and borough.
      
Rate is given as per thousand population, and are calculated using mid-year population from the first part of the financial year eg For Financial year 2008-09, mid-year estimates for 2008 are used.

Offences: These are confirmed reports of crimes being committed. All data relates to "notifiable offences" - which are designated categories of crimes that all police forces in England and Wales are required to report to the Home Office Crime rates are not available for Heathrow due to no population figures

There were changes to the police recorded crime classifications from April 2012. Therefore caution should be used when comparing sub-groups of crime figures from 2012/13 with earlier years.

Action Fraud have taken over the recording of fraud offences on behalf of individual police forces. This process began in April 2011 and was rolled out to all police forces by March 2013. Due to this change caution should be applied when comparing data over this transitional period and with earlier years.

## Data
Dataset used for this scraping have been found on [Recorderd crime: Borugh Rates](https://data.london.gov.uk/dataset/recorded_crime_rates).
 
Output data is located in `data` directory, it consists of two `csv` files:
* `crime-rates.csv`
* `recorded-offences.csv`

## Preparation
You will need Python 3.6 or greater and dataflows library to run the script

To update the data run the process script locally:

```
# Install dataflows
pip install dataflows

# Run the script
python london-crime.py
```

### License

Open Government Licence

> You are encouraged to use and re-use the Information that is available under this licence freely and flexibly, with only a few conditions.
Using Information under this licence
>Use of copyright and database right material expressly made available under this licence (the 'Information') indicates your acceptance of the terms and conditions below.
> The Licensor grants you a worldwide, royalty-free, perpetual, non-exclusive licence to use the Information subject to the conditions below.
> This licence does not affect your freedom under fair dealing or fair use or any other copyright or database right exceptions and limitations.

You may find further information [here](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

