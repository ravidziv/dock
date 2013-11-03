Dock
====

A library for moving data, based on Python, Git, and good intentions.

Dock provides a simple set of interfaces and conventions to work with data that is in human readable and writable formats, like CSV, and pragmatically write it to a datastore.

Dock works with supported ORM (currently, Django's ORM).

Dock aims to handle the whole flow of data in an application:

CMS <> Git Repository <> Datastore

Dock is ideally suited for open data projects, and projects that feature a lot of table-like data structures.

We refer to CMS in a non-traditional sense. We think tools like Google Drive can provide a vastly improved CMS-like experience than common CMS interfaces, for table-like data, and use cases where many content editors need to work on shared data sets.


API
===

Dock introspects fields via the ORM, and knows how to import data. To do this, there are a couple simple but important conventions.

File structure:

* Each file should be a model, and named with the name of the model (eg: Book() > book.csv)
* Each directory of files should be the name of the module that contains the modules (eg: books.Book() & books.Chapter() > ~/books/book.csv * ~/books/chapter.csv)
* Each directory has an index file that is in json, and has an "ordering" key. ordering is an array of names of either files or directories, and declares the order that the data must be saved to the datastore. This is how all dependencies are ensured at save time.

Naming:

Data headers




Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

