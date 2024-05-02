from spaceone.inventory.plugin.collector.model.field import *
from spaceone.inventory.plugin.collector.model.dynamic_view import *


class MetadataGenerator:
    def __init__(self, metadata: dict):
        self.metadata = metadata

    def generate_metadata(self) -> dict:
        (
            query_sets_meta,
            search_meta,
            table_meta,
            tabs_meta,
            widget_meta,
        ) = self._separate_metadata()

        metadata = {
            "view": {
                "search": self._generate_search(search_meta),
                "table": self._generate_table(table_meta),
                "sub_data": self._generate_tabs(tabs_meta),
            }
        }

        if widget_meta:
            metadata["view"].update({"widget": widget_meta})

        if query_sets_meta:
            metadata["query_sets"] = query_sets_meta

        return metadata

    def _separate_metadata(self):
        old_metadata_keys = list(self.metadata.keys())

        query_sets_meta = self.metadata.get("query_sets", {})
        search_meta = self.metadata.get("search", {})
        table_meta = self.metadata.get("table", {})
        widget_meta = self.metadata.get("widget", {})

        tabs_meta = {}
        for key in old_metadata_keys:
            if key.startswith("tabs."):
                prefix, index = key.split(".", 1)
                tabs_meta[index] = self.metadata[key]

        tabs_meta = sorted(tabs_meta.items())
        return query_sets_meta, search_meta, table_meta, tabs_meta, widget_meta

    def _generate_search(self, search_meta: dict) -> list:
        return self._generate_fields(search_meta["fields"])

    def _generate_table(self, table_meta: dict) -> dict:
        table_metadata = self._generate_default_dynamic_view(
            "Main Table", "query-search-table"
        )

        if "sort" in table_meta:
            table_metadata["options"]["default_sort"] = self._generate_sort(table_meta)

        if "fields" in table_meta:
            table_metadata["options"]["fields"] = self._generate_fields(
                table_meta["fields"]
            )
        return {"layout": MainTableDynamicView(**table_metadata).dict()}

    def _generate_tabs(self, tabs_meta: list) -> dict:
        new_tabs_metadata = []

        for tab in tabs_meta:
            index, tab_meta = tab

            # generate multi dynamic view
            if "items" in tab_meta:
                dynamic_view = self._generate_default_dynamic_view(
                    name=tab_meta["name"], view_type=tab_meta.get("type", "list")
                )

                inner_dynamic_views = []
                for inner_tab_meta in tab_meta["items"]:
                    inner_dynamic_view = self._generate_default_dynamic_view(
                        name=inner_tab_meta["name"],
                        view_type=inner_tab_meta["type"],
                    )

                    if "markdown" in inner_tab_meta["type"]:
                        inner_dynamic_view["options"]["markdown"] = inner_tab_meta[
                            "root_path"
                        ]
                        del inner_tab_meta["root_path"]

                    if "sort" in inner_tab_meta:
                        inner_dynamic_view["options"]["default_sort"] = (
                            self._generate_sort(inner_tab_meta["sort"])
                        )

                    if "root_path" in inner_tab_meta:
                        inner_dynamic_view["options"]["root_path"] = inner_tab_meta[
                            "root_path"
                        ]

                    if "fields" in inner_tab_meta:
                        inner_dynamic_view["options"]["fields"] = self._generate_fields(
                            inner_tab_meta["fields"]
                        )

                    inner_dynamic_views.append(inner_dynamic_view)
                    dynamic_view["options"]["layouts"] = inner_dynamic_views
                new_tabs_metadata.append(dynamic_view)

            # generate single dynamic view
            elif "fields" in tab_meta:
                dynamic_view = self._generate_default_dynamic_view(
                    name=tab_meta["name"], view_type=tab_meta["type"]
                )

                dynamic_view["options"]["fields"] = self._generate_fields(
                    tab_meta["fields"]
                )

                if "root_path" in tab_meta:
                    dynamic_view["options"]["root_path"] = tab_meta["root_path"]

                new_tabs_metadata.append(dynamic_view)
        return {"layouts": new_tabs_metadata}

    @staticmethod
    def _generate_default_dynamic_view(name, view_type="list", options=None):
        if options is None:
            options = {}

        return {"name": name, "type": view_type, "options": options}

    @staticmethod
    def _generate_sort(table: dict) -> dict:
        sort_field = table["sort"]

        sort_option = {
            "key": sort_field["key"],
        }

        if "desc" in sort_field:
            sort_option["desc"] = sort_field["desc"]

        return Sort(**sort_option).dict()

    def _generate_fields(self, fields: list) -> list:
        gen_fields = []
        for field in fields:

            if "type" not in field:
                gen_fields.append(self._generate_text_field(field))

            elif field["type"] == "text":
                gen_fields.append(self._generate_text_field(field))

            elif field["type"] == "dict":
                gen_fields.append(self._generate_dict_field(field))

            elif field["type"] == "size":
                gen_fields.append(self._generate_size_field(field))

            elif field["type"] == "progress":
                gen_fields.append(self._generate_progress_field(field))

            elif field["type"] == "datetime":
                gen_fields.append(self._generate_datetime_field(field))

            elif field["type"] == "state":
                gen_fields.append(self._generate_state_field(field))

            elif field["type"] == "badge":
                gen_fields.append(self._generate_badge_field(field))

            elif field["type"] == "image":
                gen_fields.append(self._generate_image_field(field))

            elif field["type"] == "enum":
                gen_fields.append(self._generate_enum_field(field))

            elif field["type"] == "more":
                gen_fields.append(self._generate_more_field(field))
        return gen_fields

    def _generate_text_field(self, field: dict) -> dict:
        if "key" not in field:
            field = self._add_key_name_fields(field)

        if "label" not in field:
            field["type"] = "text"

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        if "default" in field:
            field["default"] = str(field["default"])
            field = self._add_options_field(field, "default")

        if "field_description" in field:
            field = self._add_options_field(field, "field_description")

        if "reference_key" in field:
            field["reference"] = {
                "reference_key": field["reference_key"],
                "resource_type": "inventory.CloudService",
            }
            del field["reference_key"]

        if "labels" in field:
            enums = {}
            for label in field["labels"]:
                main_key = [key for key in label.keys()][0]
                label["label"] = label[main_key]
                del label[main_key]
                enums[main_key] = label
            field["enums"] = enums
            del field["labels"]

        return TextField(**field).dict(exclude_none=True)

    def _generate_dict_field(self, field: dict) -> dict:
        if "key" not in field:
            field = self._add_key_name_fields(field)

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        return DictField(**field).dict(exclude_none=True)

    def _generate_size_field(self, field: dict) -> dict:
        if "key" not in field:
            field = self._add_key_name_fields(field)

        if "display_unit" in field:
            field = self._add_options_field(field, "display_unit")

        if "source_unit" in field:
            field = self._add_options_field(field, "source_unit")

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        return SizeField(**field).dict(exclude_none=True)

    def _generate_progress_field(self, field: dict) -> dict:
        if "key" not in field:
            field = self._add_key_name_fields(field)

        if "unit" in field:
            field = self._add_options_field(field, "unit")

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        return ProgressField(**field).dict(exclude_none=True)

    def _generate_datetime_field(self, field: dict, is_enum=False) -> dict:
        if not is_enum:
            if "key" not in field:
                field = self._add_key_name_fields(field)

        if "source_type" in field:
            field = self._add_options_field(field, "source_type")

        if "source_format" in field:
            field = self._add_options_field(field, "source_format")

        if "display_format" in field:
            field = self._add_options_field(field, "display_format")

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        if not is_enum:
            return DatetimeField(**field).dict(exclude_none=True)
        else:
            return EnumDatetimeField(**field).dict(exclude_none=True)

    def _generate_state_field(self, field: dict, is_enum: bool = False) -> dict:
        if not is_enum:
            if "key" not in field:
                field = self._add_key_name_fields(field)

        if "text_color" in field:
            field = self._add_options_field(field, "text_color")

        if "icon_image" in field:
            field = self._add_options_field(
                field,
                field_name="icon_image",
                nested_field_name="icon",
                change_field_name="image",
            )

        if "icon_color" in field:
            field = self._add_options_field(
                field,
                field_name="icon_color",
                nested_field_name="icon",
                change_field_name="color",
            )

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        if not is_enum:
            return StateField(**field).dict(exclude_none=True)
        else:
            return EnumStateField(**field).dict(exclude_none=True)

    def _generate_badge_field(self, field: dict, is_enum: bool = False) -> dict:
        if not is_enum:
            if "key" not in field:
                field = self._add_key_name_fields(field)

        if "text_color" in field:
            field = self._add_options_field(field, "text_color")

        if "shape" in field:
            field = self._add_options_field(field, "shape")

        if "outline_color" in field:
            field = self._add_options_field(field, "outline_color")

        if "background_color" in field:
            field = self._add_options_field(field, "background_color")

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        if not is_enum:
            return BadgeField(**field).dict(exclude_none=True)
        else:
            return EnumBadgeField(**field).dict(exclude_none=True)

    def _generate_image_field(self, field: dict, is_enum: bool = False) -> dict:
        if not is_enum:
            if "key" not in field:
                field = self._add_key_name_fields(field)

        if "width" in field:
            field = self._add_options_field(field, "width")

        if "height" in field:
            field = self._add_options_field(field, "height")

        if "image_url" in field:
            field = self._add_options_field(field, "image_url")

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        if not is_enum:
            return ImageField(**field).dict(exclude_none=True)
        else:
            return EnumImageField(**field).dict(exclude_none=True)

    def _generate_enum_field(self, field: dict) -> dict:
        if "key" not in field:
            field = self._add_key_name_fields(field)

        if "is_optional" in field:
            field = self._add_options_field(field, "is_optional")

        if "enums" in field:
            enable_enum_options = [
                "back_ground_color",
                "text_color",
                "shape",
                "icon_color",
                "icon_image",
                "width",
                "height",
                "source_format",
                "display_format",
                "type",
            ]

            enums = {}
            for enum in field["enums"]:
                if "type" not in enum:
                    enum["type"] = "badge"

                main_key = [
                    key for key in enum.keys() if key not in enable_enum_options
                ][0]

                if enum["type"] == "badge":
                    enum["outline_color"] = enum[main_key]
                    del enum[main_key]
                    enums[main_key] = self._generate_badge_field(
                        field=enum, is_enum=True
                    )

                elif enum["type"] == "state":
                    enum["icon_color"] = enum[main_key]
                    del enum[main_key]
                    enums[main_key] = self._generate_state_field(
                        field=enum, is_enum=True
                    )

                elif enum["type"] == "image":
                    enum["image_url"] = enum[main_key]
                    del enum[main_key]
                    enums[main_key] = self._generate_image_field(
                        field=enum, is_enum=True
                    )

                elif enum["type"] == "datetime":
                    enum["source_type"] = enum[main_key]
                    del enum[main_key]
                    enums[main_key] = self._generate_datetime_field(
                        field=enum, is_enum=True
                    )

            if "options" in field:
                field["options"].update(enums)
            else:
                field["options"] = enums

                del field["enums"]

        return EnumField(**field).dict(exclude_none=True)

    def _generate_more_field(self, field: dict) -> dict:
        if "key" not in field:
            field = self._add_key_name_fields(field)

        if "popup_key" in field:
            field["options"] = {"sub_key": field["popup_key"]}
            del field["popup_key"]

        if "popup_name" in field:
            field = self._add_options_field(
                field=field,
                field_name="popup_name",
                nested_field_name="layout",
                change_field_name="name",
            )
            field["options"]["layout"]["type"] = "popup"

        if "popup_type" in field:
            field["options"]["layout"]["options"] = {
                "type": "popup",
                "layout": {"type": field["popup_type"]},
            }
            del field["popup_type"]

        if "popup_fields" in field:
            field["options"]["layout"]["options"]["layout"]["options"] = {
                "fields": self._generate_fields(field["popup_fields"])
            }
            del field["popup_fields"]

            inner_popup_items = field["options"]["layout"]["options"]["layout"]

            if "popup_table_key" in field and inner_popup_items["type"] in [
                "simple-table",
                "table",
                "search-query-table",
            ]:
                inner_popup_items["options"]["root_path"] = field["popup_table_key"]
                del field["popup_table_key"]

        return MoreField(**field).dict(exclude_none=True)

    @staticmethod
    def _add_key_name_fields(field: dict) -> dict:
        key = [key for key in field.keys() if key != "type"][0]
        field["name"] = key
        field["key"] = field[key]

        del field[key]
        return field

    @staticmethod
    def _add_options_field(
        field: dict,
        field_name: str,
        nested_field_name=None,
        change_field_name=None,
    ) -> dict:
        if not nested_field_name:
            if "options" in field:
                field["options"][field_name] = field[field_name]
            else:
                field["options"] = {field_name: field[field_name]}

            del field[field_name]
        else:
            if "options" in field:
                if not change_field_name:
                    if nested_field_name in field["options"]:
                        field["options"][nested_field_name][field_name] = field[
                            field_name
                        ]
                        del field[field_name]
                    else:
                        field["options"][nested_field_name] = {
                            field_name: field[field_name]
                        }
                        del field[field_name]
                else:
                    if nested_field_name in field["options"]:
                        field["options"][nested_field_name][change_field_name] = field[
                            field_name
                        ]
                        del field[field_name]
                    else:
                        field["options"][nested_field_name] = {
                            change_field_name: field[field_name]
                        }
                        del field[field_name]
            else:
                if not change_field_name:
                    field["options"] = {
                        nested_field_name: {field_name: field[field_name]}
                    }
                    del field[field_name]
                else:
                    field["options"] = {
                        nested_field_name: {change_field_name: field[field_name]}
                    }
                    del field[field_name]

        return field
