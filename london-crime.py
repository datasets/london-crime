from dataflows import Flow, ResourceWrapper, PackageWrapper, load, dump_to_path, unpivot, delete_fields


def set_format_and_name_crime(package: PackageWrapper):

    package.pkg.descriptor['title'] = 'Crime'
    package.pkg.descriptor['name'] = 'crime'

    package.pkg.descriptor['licenses'] = [{
        "name": "OGL",
        "path": 'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/',
        "title": 'Open Government Licence'
    }]

    package.pkg.descriptor['resources'][0]['path'] = 'data/crime-rates.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'crime-rates'

    package.pkg.descriptor['resources'][1]['path'] = 'data/recorded-offences.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'recorded-offences'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    yield first.it
    yield second.it
    yield from package

def remove_duplicates(rows):
    seen = set()
    for row in rows:
        line = ''.join('{}{}'.format(key, val) for key, val in row.items())
        if line in seen: continue
        seen.add(line)
        yield row

link = 'https://data.london.gov.uk/download/recorded_crime_rates/25f3c04c-898a-41c9-b911-93cac6df205f' \
       '/met-police-recorded-offences-rates-borough.xlsx'

unpivot_fields = [
    {'name': 'All recorded offences 1999-00', 'keys': {'Year': '1999-12-31'}},
    {'name': 'All recorded offences 2000-01', 'keys': {'Year': '2000-12-31'}},
    {'name': 'All recorded offences 2001-02', 'keys': {'Year': '2001-12-31'}},
    {'name': 'All recorded offences 2002-03', 'keys': {'Year': '2002-12-31'}},
    {'name': 'All recorded offences 2003-04', 'keys': {'Year': '2003-12-31'}},
    {'name': 'All recorded offences 2004-05', 'keys': {'Year': '2004-12-31'}},
    {'name': 'All recorded offences 2005-06', 'keys': {'Year': '2005-12-31'}},
    {'name': 'All recorded offences 2006-07', 'keys': {'Year': '2006-12-31'}},
    {'name': 'All recorded offences 2007-08', 'keys': {'Year': '2007-12-31'}},
    {'name': 'All recorded offences 2008-09', 'keys': {'Year': '2008-12-31'}},
    {'name': 'All recorded offences 2009-10', 'keys': {'Year': '2009-12-31'}},
    {'name': 'All recorded offences 2010-11', 'keys': {'Year': '2010-12-31'}},
    {'name': 'All recorded offences 2011-12', 'keys': {'Year': '2011-12-31'}},
    {'name': 'All recorded offences 2012-13', 'keys': {'Year': '2012-12-31'}},
    {'name': 'All recorded offences 2013-14', 'keys': {'Year': '2013-12-31'}},
    {'name': 'All recorded offences 2014-15', 'keys': {'Year': '2014-12-31'}},
    {'name': 'All recorded offences 2015-16', 'keys': {'Year': '2015-12-31'}},
    {'name': 'All recorded offences 2016-17', 'keys': {'Year': '2016-12-31'}}
]
extra_keys = [
    {'name': 'Year', 'type': 'date'}
]
extra_value = {'name': 'Value', 'type': 'any'}

Flow(
    load(link,
         format="xlsx",
         headers=[1, 2],
         fill_merged_cells=True,
         skip_rows=['', 3],
         sheet=2),
    load(link,
         format="xlsx",
         headers=[1, 2],
         fill_merged_cells=True,
         skip_rows=['', 3],
         sheet=3),
    delete_fields(['Violence Against the Person 1999-00', 'Violence Against the Person 2000-01', 'Violence Against the Person 2001-02', 'Violence Against the Person 2002-03', 'Violence Against the Person 2003-04', 'Violence Against the Person 2004-05', 'Violence Against the Person 2005-06', 'Violence Against the Person 2006-07', 'Violence Against the Person 2007-08', 'Violence Against the Person 2008-09', 'Violence Against the Person 2009-10', 'Violence Against the Person 2010-11', 'Violence Against the Person 2011-12', 'Violence Against the Person 2012-13', 'Violence Against the Person 2013-14', 'Violence Against the Person 2014-15', 'Violence Against the Person 2015-16', 'Violence Against the Person 2016-17', 'Sexual Offences 1999-00', 'Sexual Offences 2000-01', 'Sexual Offences 2001-02', 'Sexual Offences 2002-03', 'Sexual Offences 2003-04', 'Sexual Offences 2004-05', 'Sexual Offences 2005-06', 'Sexual Offences 2006-07', 'Sexual Offences 2007-08', 'Sexual Offences 2008-09', 'Sexual Offences 2009-10', 'Sexual Offences 2010-11', 'Sexual Offences 2011-12', 'Sexual Offences 2012-13', 'Sexual Offences 2013-14', 'Sexual Offences 2014-15', 'Sexual Offences 2015-16', 'Sexual Offences 2016-17', 'Robbery 1999-00', 'Robbery 2000-01', 'Robbery 2001-02', 'Robbery 2002-03', 'Robbery 2003-04', 'Robbery 2004-05', 'Robbery 2005-06', 'Robbery 2006-07', 'Robbery 2007-08', 'Robbery 2008-09', 'Robbery 2009-10', 'Robbery 2010-11', 'Robbery 2011-12', 'Robbery 2012-13', 'Robbery 2013-14', 'Robbery 2014-15', 'Robbery 2015-16', 'Robbery 2016-17', 'Burglary 1999-00', 'Burglary 2000-01', 'Burglary 2001-02', 'Burglary 2002-03', 'Burglary 2003-04', 'Burglary 2004-05', 'Burglary 2005-06', 'Burglary 2006-07', 'Burglary 2007-08', 'Burglary 2008-09', 'Burglary 2009-10', 'Burglary 2010-11', 'Burglary 2011-12', 'Burglary 2012-13', 'Burglary 2013-14', 'Burglary 2014-15', 'Burglary 2015-16', 'Burglary 2016-17', 'Theft and Handling 1999-00', 'Theft and Handling 2000-01', 'Theft and Handling 2001-02', 'Theft and Handling 2002-03', 'Theft and Handling 2003-04', 'Theft and Handling 2004-05', 'Theft and Handling 2005-06', 'Theft and Handling 2006-07', 'Theft and Handling 2007-08', 'Theft and Handling 2008-09', 'Theft and Handling 2009-10', 'Theft and Handling 2010-11', 'Theft and Handling 2011-12', 'Theft and Handling 2012-13', 'Theft and Handling 2013-14', 'Theft and Handling 2014-15', 'Theft and Handling 2015-16', 'Theft and Handling 2016-17', 'Fraud or Forgery 1999-00', 'Fraud or Forgery 2000-01', 'Fraud or Forgery 2001-02', 'Fraud or Forgery 2002-03', 'Fraud or Forgery 2003-04', 'Fraud or Forgery 2004-05', 'Fraud or Forgery 2005-06', 'Fraud or Forgery 2006-07', 'Fraud or Forgery 2007-08', 'Fraud or Forgery 2008-09', 'Fraud or Forgery 2009-10', 'Fraud or Forgery 2010-11', 'Fraud or Forgery 2011-12', 'Fraud or Forgery 2012-13', 'Fraud or Forgery 2013-14', 'Fraud or Forgery 2014-15', 'Fraud or Forgery 2015-16', 'Fraud or Forgery 2016-17', 'Criminal Damage 1999-00', 'Criminal Damage 2000-01', 'Criminal Damage 2001-02', 'Criminal Damage 2002-03', 'Criminal Damage 2003-04', 'Criminal Damage 2004-05', 'Criminal Damage 2005-06', 'Criminal Damage 2006-07', 'Criminal Damage 2007-08', 'Criminal Damage 2008-09', 'Criminal Damage 2009-10', 'Criminal Damage 2010-11', 'Criminal Damage 2011-12', 'Criminal Damage 2012-13', 'Criminal Damage 2013-14', 'Criminal Damage 2014-15', 'Criminal Damage 2015-16', 'Criminal Damage 2016-17', 'Drugs 1999-00', 'Drugs 2000-01', 'Drugs 2001-02', 'Drugs 2002-03', 'Drugs 2003-04', 'Drugs 2004-05', 'Drugs 2005-06', 'Drugs 2006-07', 'Drugs 2007-08', 'Drugs 2008-09', 'Drugs 2009-10', 'Drugs 2010-11', 'Drugs 2011-12', 'Drugs 2012-13', 'Drugs 2013-14', 'Drugs 2014-15', 'Drugs 2015-16', 'Drugs 2016-17', 'Other Notifiable Offences 1999-00', 'Other Notifiable Offences 2000-01', 'Other Notifiable Offences 2001-02', 'Other Notifiable Offences 2002-03', 'Other Notifiable Offences 2003-04', 'Other Notifiable Offences 2004-05', 'Other Notifiable Offences 2005-06', 'Other Notifiable Offences 2006-07', 'Other Notifiable Offences 2007-08', 'Other Notifiable Offences 2008-09', 'Other Notifiable Offences 2009-10', 'Other Notifiable Offences 2010-11', 'Other Notifiable Offences 2011-12', 'Other Notifiable Offences 2012-13', 'Other Notifiable Offences 2013-14', 'Other Notifiable Offences 2014-15', 'Other Notifiable Offences 2015-16', 'Other Notifiable Offences 2016-17']),
    set_format_and_name_crime,
    unpivot(unpivot_fields, extra_keys, extra_value),
    remove_duplicates,
    dump_to_path()
).process()
