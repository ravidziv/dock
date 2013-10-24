import os
import json
from itertools import chain
import tablib
from django.db.models.loading import get_model
from dock import config


class Store(object):

    """Takes a model and an object, and saves to the data store.

    Can be subclassed to provide custom save methods following the
    convention of _save_{model_name_lower_case}

    """

    def __init__(self, model, obj):

        self.model = model
        self.obj = obj

    def save(self):

        # see if there is a custom save method available for the current object,
        # else fall back to the default save method.
        try:
            save_method = getattr(self, '_save_' + self.model.__name__.lower())

        except AttributeError as e:
            save_method = self._save_base

        return save_method(**self.obj)

    # THE TEMPLATE FOR HOW A CUSTOM SAVE METHOD SHOULD LOOK
    # def _save_{model_name_lower_case}(self, **obj):
    #
    #     ##########################################################
    #     HERE is the place for any custom code to clean the item
    #     before passing it to _save_base_.
    #     After doing the custom work, ensure you have an object that
    #     can be passed to _save_base
    #
    #     If your work replaces some or all of the _prepare_obj
    #     functionality, you'll need
    #     to pass prepare=False
    #     ##########################################################
    #
    #     return self._save_base(prepare=False, **obj)

    def _save_base(self, prepare=True, **obj):

        if prepare:
            obj = self._prepare_obj(obj)

        # by convention, if we have a value for ID, we expect
        # to be able to retrieve that object.
        if 'id' in obj and obj['id']:
            try:
                instance = self.model.objects.get(pk=obj['id'])
                for k, v in obj.iteritems():
                    instance[k] = v

                obj = instance.save()

            except self.model.DoesNotExist as e:
                raise e

        else:
            obj = self.model.objects.create(**obj)

        return obj

    def _prepare_obj(self, **obj):

        for header in obj.keys():
            fields = config.DOCK_RELATION_LOOKUP_FIELDS

            if config.DOCK_HEADER_ARGS_SEPARATOR in header:
                header, field = header.split(config.DOCK_HEADER_ARGS_SEPARATOR)
                fields.insert(0, field)

            # LOGIC FOR FOREIGN KEY FIELDS DEFINED ON THE MODEL
            if self.model._meta.get_field(header).get_internal_type() == "ForeignKey":
                model = self.model._meta.get_field(header).rel.to

                for field in fields:
                    try:
                        obj[header] = model.objects.get(**{field: obj[header]})
                        break
                    except model.DoesNotExist as e:
                        raise e

            # LOGIC FOR MANYTOMANY FIELDS DEFINED ON THE MODEL

            # LOGIC FOR REVERSE FOREIGN KEY FIELDS

        return obj


class Process(object):

    """Takes data, as list of tuples, validates, and saves to the data store.

    Each tuple passed in the list has the following signature:

    (module, model, source)

    Where:

    * *module* describes a python module in the project that holds *model*
    * *source* is the file with data for *model*

    From this tuple, we map the data to a model and it's save method.

    """

    def __init__(self, freight, storage_class=Store):

        if not isinstance(freight, (list, tuple)):
            raise AssertionError("Store requires a list or a tuple, you passed neither.")

        self.freight = freight
        self.storage_class = storage_class
        self.save()

    def processed(self):
        """Extract data from the source files, clean it, and return a list of (model, obj_dict) tuples."""

        processed = []

        for box in self.freight:
            model, data = box
            raw_dataset = self._extract_data(data)
            data = self._clean_data(raw_dataset)
            processed.append((model, data))

        return processed

    def save(self):
        """Unpack our processed data and pass each object to storage class for saving."""

        for item in self.processed():
            model, data = item
            for obj in data:
                store = self.storage_class(model, obj)
                obj = store.save()

    def _extract_data(self, source):
        """Create a Dataset object from the data source."""

        with open(source) as f:
            stream = f.read()
            raw_dataset = tablib.import_set(stream)

        return raw_dataset

    def _clean_data(self, raw_dataset):
        """Takes the raw Dataset and cleans it up."""

        dataset = self._normalize_headers(raw_dataset)
        data = self._normalize_objects(dataset)

        return data

    def _normalize_headers(self, dataset):
        """Clean up the headers of each Dataset."""

        symbols = {
            # Note: We are now allowing the "_" symbol which is valid in python vars.
            ord('-'): None,
            ord('"'): None,
            ord(' '): None,
            ord("'"): None,
        }

        for index, header in enumerate(dataset.headers):
            tmp = unicode(header).translate(symbols).lower()
            dataset.headers[index] = tmp

        return dataset

    def _normalize_objects(self, dataset):
        """Clean up each object in the Dataset."""

        data = dataset.dict

        for item in data:
            for k, v in item.iteritems():
                if not v:
                    del item[k]

        return data


class Unload(object):

    """Extracts a full dataset from a path, and sorts the data for further processing.

    Unload walks the dataset directories, top down, from a root directory.

    Using the index files, which declare the *order* that the data should
    eventually be loaded, and file + directory naming conventions, which
    declare the modules and models that the data is intended for, we
    return a list of tuples, where each tuple has the target Model, and the
    path to the data for that model.

    This list is returned by the `freight` method, and is intended to be consumed
    by the Process class, which further processed the data and prepares it for
    saving to the data store.

    """

    def __init__(self, data_root, ignore_dirs=('assets',), index_file='index.json', supported_extensions=('.csv',)):

        self.data_root = data_root
        self.ignore_dirs = set(ignore_dirs)
        self.index_file = index_file
        self.supported_extensions = supported_extensions
        self.root_index = self.data_root + '/' + index_file

    def walk_and_sort(self):
        """Returns a list of data sources, ordered by desired save order."""

        ordered_branches = []
        ordered_sources = []
        sources = []

        for root, dirs, files in os.walk(self.data_root):

            # only consider roots that have an index file
            if self.index_file in files:
                root_index = os.path.join(root, self.index_file)

                with open(root_index) as f:
                    index = dict(json.load(f))
                    # get the ordering for this scope
                    index = index['ordering']

                    # TODO: handle case of directory and file in same scope with the same name
                    # TODO: handle case of two files with the same name and different supported extensions
                    # TODO: Check the implementation for multiple nested directories and rewrite accordingly
                    for entry in index:
                        entry_path = os.path.join(root, entry)

                        # build an ordered list of data branches from the indexed directories
                        if os.path.exists(entry_path) and entry_path not in ordered_branches:
                            ordered_branches.append(entry_path)

                        # build a list of data sources, from the indexed files
                        for ext in self.supported_extensions:
                            source_path = entry_path + ext
                            if os.path.exists(source_path):
                                sources.append(source_path)

        ordered_sources.extend(ordered_branches)

        # each ordered branch will be replaced with an ordered list of the files it contains
        for index, branch in enumerate(ordered_sources):
            ordered_sources[index] = [source for source in sources if source.startswith(branch)]

        # return a flattened list that is ordered for loading to the data store
        return list(chain.from_iterable(ordered_sources))

    def freight(self):
        """Takes the ordered list of data sources, and builds a new list of tuples, with (model, data_source).

        Getting the Model relies on a naming convention in the dataset:
        * the data_source filename is the destination Model name, in lowercase, and the Model itself should be in title case.
        * the data_source parent directory is the module that holds the Model

        """

        freight = []

        for data_source in self.walk_and_sort():
            this_path, ext = os.path.splitext(data_source)
            path_elements = this_path.split('/')
            model_name = path_elements[-1]
            module_name = path_elements[-2]
            # TODO: Remove django's get model with a wrapper that will do the same for whatever backend
            model = get_model(module_name, model_name.title())
            freight.append((model, data_source))

        return freight
