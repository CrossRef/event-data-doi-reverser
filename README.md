# Event Data DOI Reverser

Follow DOIs. Produce Artifacts. Reverse URLs into DOIs.

## Operation - Collecting data.

### Back-fill DOIs

Back-fill DOIs to catch up with all DOIs. Date format is that understood by Crossref MDAPI (i.e. `YYYY`, `YYYY-MM` or `YYYY-MM-DD`). 

e.g.

    lein with-profile dev run update-items-many 2016-01 2016-02 2016-03 2016-04 2016-05 2016-06 2016-07 2016-07 2016-08 2016-09 2016-10

### Update DOIs

Daily, update the DOIs.

    TODO

### Update Resource URLs

Crawl every Item's DOI to find the Resource URL. This will run until it's finished.

    lein with-profile dev run update-resource-urls

Run this once after back-fill.
Then run every day.

### Sample Eventual URLs per Domain

Take a sample of every referrer domain's resource URLs. Record naïve redirects and browser-based redirects. 

    TODO

Run this once after back-fill.
Then run every day.

### Naïve follow eventual URLs

For those domains that require naïve redirects, follow resource URLs for all Items where data is missing.

    TODO

Run this once after back-fill.
Then run every day.

### Browser follow eventual URLs

For those domains that require browser redirects, follow resource URLs for all Items where data is missing.

    TODO

Run this once after back-fill.
Then run every day.

### Detect conflicts.

    TODO

Run every day.

### Export the Domain List Artifact

Create the Artifact for the domain list, archive it, and upload to the Evidence Service.

    TODO

Run this once a week.

### Export the URL List Artifact

Create the Artifact for the URL DOI list, archive it, and upload to the Evidence Service.

    TODO

Run this once a week.

## Operation - Running server

Run the service.

    lein with-profile dev run server



## Behaviour

The service returns only valid, existing DOIs. It accepts the following input:

 - landing page URL
 - DOI
 - free text

And returns a DOI. The DOI that is returned is the 'canonical' version, which means that if a valid DOI is passed in, you may get a different DOI out if there is a conflict. The following rules are followed:

 - if an item is aliased to another, the DOI of other will be retured
 - if two items are registered for the metdata and the abstract of a work (this can happen with SICIs), the DOI for the metdata is returned


### Back-fill

Run a back-fill process in parallel for a number of dates, e.g. 

    lein with-profile dev run update-items-many 2016-01 2016-02 2016-03 2016-04 2016-05 2016-06 2016-07 2016-07 2016-08 2016-09 2016-10

### TODO

Follow link shorteners, try reverse at each stage. e.g. https://t.co/VIXpgGrl8p

 Sanity check
 - no aliased doi is alias of another
 - no doi.org resource urls
