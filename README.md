# aim_xml
Contains code base to work on Bloomberg XML trade file


## repo_upload.py
This module is responsible for uploading Bloomberg AIM repo XML files to Geneva. It accepts 5 types of XML files:

1. master
2. trade
3. rerate
4. dummy_rerate
5. resize

For master, trade, rerate, and dummy_rerate types of files, it will add appropriate XML headers and footers and upload them to Geneva. For resize, it will do nothing.

For all types of files, the program will send a notification email to indicate the status of conversion, then move them to the SENT folder.



## repo_datastore.py
This module is reponsible for reading Bloomberg AIM XML files and save their information to the datastore. It accepts 3 types of XML files:

1. master
2. trade
3. rerate

For each type of XML file, it will read its information and call datastore APIs to store them into datastore.


## dummy_rerate.py
This module is responsible for AIM repo XML trade files and create dummy_rerate files for all the new repo positions of type OPEN. Those dummy rerate files do not change the interest rate or settlement date of the new repo positions, but to tell Geneva that they are of variable rate.


## Upload logic
Flowing all repo trades to Geneva takes 6 steps:

1. Upload master file
2. Create dummy rerate file
3. Upload trade file
4. Upload dummy rerate file
5. Upload rerate file
6. Check resize file

There should be at least 10 minutes time interval between task 1, 3, 4, and 5 to make sure Geneva load them in order.


