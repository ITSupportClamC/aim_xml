# aim_xml
Contains code base to work on Bloomberg XML trade file


## repo_upload.py
This module is responsible for uploading Bloomberg AIM XML files to Geneva. It accepts 4 types of XML files:

1. master: search for repo master file;
2. trade: search for repo trade file;
3. rerate: search for repo rerate file;
4. resize: search for repo resize file.

For master, trade, and rerate types of files, it will add appropriate XML headers and footers and upload them to Geneva. For resize, it will do nothing.

For all types of files, the program will send a notification email to indicate the status of conversion, then move them to the SENT folder.



## repo_datastore.py
This module is reponsible for reading Bloomberg AIM XML files and save their information to the datastore. It accepts 3 types of XML files:

1. master: search for repo master file;
2. trade: search for repo trade file;
3. rerate: search for repo rerate file.

For each type of XML file, it will read its information and call datastore APIs to store them into datastore.
