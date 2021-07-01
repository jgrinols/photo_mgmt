- physical file metadata will be imported into Piwigo by sync process (*this will be mostly just exif data at this point*)
- agent will monitor the following db tables for metadata changes:
    - `image_tag` (inserts and deletes)
        - map the comma separated list to the `Keywords` iptc field `2#025`
    - `images` (updates)
        - `name` field in the table maps to the `Title` in the Piwigo ui. Maps to `Title` or `Object Name` IPTC field `2#005` (*first 64 characters*)
            - the piwigo metadata plugin also dupes the whole db field to the `Headline` iptc field `2#105`
        - `comment` field in the table maps to the `Description` in the Piwigo ui. Maps to the `Description` iptc field `2#120`
        - `author` field in the table maps to the `Author` field in the ui. Maps to the `Author` iptc field `2#122`

- when we receive any event, will will add it to a collection of pending image metadata updates--there should be only one record for each image id

-typical workflow:
    1) download items into /mass/piwigo/media
    2) execute piwigo synchrononization to pull new items into db
    3) in piwigo, move keepers into caddie
        - metadata agent will listen for writes to caddie table
        - queue up job(s) to auto generate tags (labels and faces)
        - queue up a job to pull in implicit tags
        - add resolved tags to db
    4) metadata agent listens for metadata changes in db
        - queue up job to apply changes to physical files
    5) 