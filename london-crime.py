from dataflows import Flow, ResourceWrapper, PackageWrapper, load, dump_to_path


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


link = 'https://data.london.gov.uk/download/recorded_crime_rates/25f3c04c-898a-41c9-b911-93cac6df205f' \
       '/met-police-recorded-offences-rates-borough.xlsx'

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
    set_format_and_name_crime,
    dump_to_path(),
).process()
