#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream

import dateutil.parser as parser

class LightspeedStream(HttpStream, ABC):

    def __init__(self, config: Dict):
        super().__init__(authenticator=config["authenticator"])
        self.config = config

    @property
    def url_base(self) -> str:
        return f"https://api.lightspeedapp.com/API/Account/{self.config['account_id']}/"

    @property
    @abstractmethod
    def data_field(self) -> str:
        """The name of the field in the response which contains the data"""

    def path(self, **kwargs) -> str:
        return f"{self.data_field}.json"

    @property
    def relationships(self) -> list:
        """
        API docs: https://developers.lightspeedhq.com/retail/introduction/relations/
        """

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        attributes = response.json().get("@attributes", None)
        if attributes:
            if "limit" in attributes and "offset" in attributes:
                offset = int(attributes['offset']) + int(attributes['limit'])
                if offset < int(response.json().get('@attributes')['count']):
                    return offset
                return None

    def request_params(
        self, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {}
        if next_page_token:
            params['offset'] = next_page_token
        if self.relationships:
            params['load_relations'] = self.relationships
        return params


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        results = response.json().get(self.data_field, []) if self.data_field is not None else response.json()
        records = results if isinstance(results, list) else [results]
        for record in records:
            yield record

class IncrementalLightspeedStream(LightspeedStream, ABC):

    order_field = "timeStamp"
    cursor_field = "timeStamp"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))}

    def request_params(self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs):
        params = super().request_params(stream_state=stream_state, next_page_token=next_page_token, **kwargs) or {}
        params['orderby'] = self.cursor_field
        if not next_page_token:
            if stream_state:
                params[self.cursor_field] = f">,{parser.parse(stream_state.get(self.cursor_field))}"
        return params

class Categories(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Category/
    """
    
    data_field = "Category"
    primary_key = "categoryID"

class Customers(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Customer/
    """
    
    data_field = "Customer"
    primary_key = "customerID"

    relationships = '["Contact"]'

class Discounts(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Discount/
    """
    
    data_field = "Discount"
    primary_key = "discountID"

class Items(IncrementalLightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Item/
    """
    
    data_field = "Item"
    primary_key = "itemID"

    relationships = '["ItemShops","ItemPrices"]'

class ItemMatrices(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/ItemMatrix/
    """
    
    data_field = "ItemMatrix"
    primary_key = "itemMatrixID"

class ItemAttributeSets(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/ItemAttributeSet/
    """
    
    data_field = "ItemAttributeSet"
    primary_key = "ItemAttributeSetID"

class Manufacturers(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Manufacturer/
    """
    
    data_field = "Manufacturer"
    primary_key = "manufacturerID"

class PaymentTypes(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/PaymentType/
    """
    
    data_field = "PaymentType"
    primary_key = "paymentTypeID"
    
class Sales(IncrementalLightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Sale/
    """
    
    data_field = "Sale"
    primary_key = "saleID"
    
class SaleLines(IncrementalLightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/SaleLine/
    """
    
    data_field = "SaleLine"
    primary_key = "saleID"

class SalePayments(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/SalePayment/
    """
    
    data_field = "SalePayment"
    primary_key = "salePaymentID"

class Shops(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/Shop/
    """
    
    data_field = "Shop"
    primary_key = "shopID"

class TaxCategories(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/TaxCategory/
    """
    
    data_field = "TaxCategory"
    primary_key = "taxCategoryID"

class TaxClasses(LightspeedStream):
    """
    API docs: https://developers.lightspeedhq.com/retail/endpoints/TaxClass/
    """
    
    data_field = "TaxClass"
    primary_key = "taxClassID"