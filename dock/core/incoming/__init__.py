import os
import json
from itertools import chain
import tablib
from django.db.models.loading import get_model
from django.db.models.fields import FieldDoesNotExist
from dock import config


class Store(object):

    """Takes a model and an object, and saves to the data store.

    Can be subclassed to provide custom save methods following the
    convention of _save_{model_name_lower_case}

    """

    def __init__(self, model, obj):

        self.model = model
        self.obj = obj
        self.direct_relation_types = [
            "ForeignKey",
            "ManyToManyField",
            "OneToOneField"
        ]

    def save(self):

        # see if there is a custom save method available for the current object,
        # else fall back to the default save method.
        try:
            save_method = getattr(self, '_save_' + self.model.__name__.lower())

        except AttributeError as e:
            save_method = self._save_base

        return save_method(**self.obj)

    def _save_base(self, prepare=True, **obj):

        if prepare:
            obj = self._prepare_obj(**obj)

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

    def _prepare_obj(self, **obj):

        for key in obj.keys():

            lookup_fields = config.DOCK_RELATION_LOOKUP_FIELDS
            related_model = None
            header = key

            if config.DOCK_HEADER_ARGS_SEPARATOR in key:
                header, header_args = key.split(config.DOCK_HEADER_ARGS_SEPARATOR)
                # TODO: improve the way we take args add add them for lookups. should be namespaced
                # TODO: also, try the accessor syntax from django `model__field_name`
                lookup_fields.insert(0, header_args)

            try:
                # If the field is actually defined on the class, we'll get the related_model like this
                model_field = self.model._meta.get_field(header)
                if model_field.get_internal_type() in self.direct_relation_types:
                    related_model = model_field.rel.to

            except FieldDoesNotExist as e:
                # The field was not on the class, it is a reverse relation in the ORM
                try:
                    model_field = getattr(self.model, header)
                    related_model = model_field.related.model

                except AttributeError as e:
                    #TODO: we actually are raising here when the try/except it is wrapped in
                    # also fails, and thus the user cant know which error to debug
                    raise e

            if related_model:

                related_success = False

                for field in lookup_fields:
                    try:
                        obj[header] = related_model.objects.get(**{field: obj[header]})
                        related_success = True
                        break

                    except related_model.DoesNotExist as e:
                        pass

                if not related_success:
                    # raising here so our loop above executes as desired
                    raise e

        return obj


class Process(object):

    """Takes data, as list of tuples, validates, and saves to the data store.

    Each tuple passed in the list has the following signature:

    (model, data_source)

    Where:

    * *module* describes a python module in the project that holds *model*
    * *data_source* is the file with data for *model*

    """

    def __init__(self, inventory, storage_class=Store, dataset_processing_class=None):

        if not isinstance(inventory, (list, tuple)):
            raise AssertionError("Store requires inventory as a list or a tuple, you passed neither.")

        if dataset_processing_class:
            if not isinstance(dataset_processing_class, type):
                raise AssertionError("Dataset Processor must be a class")

            if not hasattr(dataset_processing_class, 'processed'):
                raise AssertionError("Dataset Processor must have a callable attribute named `processed`")

        self.inventory = inventory
        self.storage_class = storage_class

        # `self.dataset_processing_class` is implemented to allow processing of the dataset as a whole,
        # for example, validations on the whole set, extracting additional datasets
        # out of the passed dataset, and so on.
        self.dataset_processing_class = dataset_processing_class
        self.save()

    def processed(self):
        """Extract data from the source files, clean headers and rows, and return a list of (model, dataset) tuples."""

        processed = []

        for item in self.inventory:
            model, data_source = item
            dataset_raw = self._extract_data(data_source)
            dataset_clean = self._clean_data(dataset_raw)
            processed.append((model, dataset_clean))

        if self.dataset_processing_class:
            # We'll send processed as the first argument of whatever class we are given
            # We expect that class to implement a processed method that returns an ordered
            # list of (model, dataset) tuples, just list Process.processed
            dataset_processor = self.dataset_processing_class(processed)
            processed = dataset_processor.processed()

        return processed

    def save(self):
        """Unpack our processed data and pass each object to storage class for saving."""

        for item in self.processed():
            model, dataset = item
            for obj in dataset:
                store = self.storage_class(model, obj)
                obj = store.save()

    def _extract_data(self, data_source):
        """Create a Dataset object from the data source."""

        with open(data_source) as f:
            stream = f.read()
            raw_dataset = tablib.import_set(stream)

        return raw_dataset

    def _clean_data(self, raw_dataset):
        """Takes the raw Dataset and cleans it up."""

        dataset_clean_headers = self._normalize_headers(raw_dataset)
        dataset_clean = self._normalize_rows(dataset_clean_headers)

        return dataset_clean

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

    def _normalize_rows(self, dataset):
        """Clean up each object in the Dataset."""

        dataset = dataset.dict

        for item in dataset:
            for k, v in item.iteritems():
                if not v:
                    del item[k]

        return dataset


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
        self.root_index = os.path.abspath(os.path.join(self.data_root, index_file))

    def extract_sources(self):
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

    def map_inventory(self):
        """Takes the extracted data sources, and builds a new list of tuples like (model, data_source).

        Getting the Model relies on a naming convention in the dataset:
        * the data_source filename is the destination Model name, in lowercase, and the Model itself should be in title case.
        * the data_source parent directory is the module that holds the Model

        """

        inventory = []

        for data_source in self.extract_sources():
            full_path, ext = os.path.splitext(data_source)
            head, model_name = os.path.split(full_path)
            head, module_name = os.path.split(head)
            # TODO: Remove django's get model with a wrapper that will do the same for whatever backend
            model = get_model(module_name, model_name.title())
            inventory.append((model, data_source))

        return inventory
