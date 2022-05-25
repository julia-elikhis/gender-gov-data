import csv
import sys

from knesset_data.dataservice.committees import Committee
from knesset_data.dataservice.bill_initiator import BillInitiator
from knesset_data.dataservice.bills import Bill
from knesset_data.dataservice.constants import SERVICE_URLS
from knesset_data.dataservice.exceptions import KnessetDataServiceObjectException
from knesset_data.dataservice.persons import Person, PersonToPosition, Position

SERVICE_URLS["api"] = SERVICE_URLS["api"].strip("/")


class FilteredCommittees(Committee):
    @classmethod
    def get_all_by_knesset_number(cls, knesset_num):
        params = {'$filter': f"KnessetNum eq {knesset_num}"}
        return cls._get_all_pages(cls._get_url_base(), params=params)


class FilteredPerson(Person):
    @classmethod
    def get_all_present_members(cls, proxies=None):
        params = {'$filter': 'IsCurrent eq true'}
        return cls._get_all_pages(cls._get_url_base(), params=params, proxies=proxies)


class FilteredPersonToPosition(PersonToPosition):

    @classmethod
    def get_all_by_knesset_number(cls, knesset_num):
        params = {
            '$filter': f"KnessetNum eq {knesset_num}",
            # '$expand': f"{Position.METHOD_NAME}",
            # '$expand': f"{Person.METHOD_NAME}"
        }
        return cls._get_all_pages(cls._get_url_base(), params=params)


class FilteredBill(Bill):
    @classmethod
    def _get_instance_from_entry(cls, entry, skip_exceptions=False):
        try:
            parsed_entry = cls._parse_entry(entry)
            if "billinitiatorid" in parsed_entry["data"]:
                return BillInitiator(parsed_entry)
            else:
                return Bill(parsed_entry)

        except Exception as e:
            if skip_exceptions:
                return KnessetDataServiceObjectException(cls, e, entry)
            else:
                raise e

    @classmethod
    def _flatten_pages(cls, all_pages):
        bills = {}
        for page in list(all_pages):
            if page.METHOD_NAME is Bill.METHOD_NAME:
                bills[page.get("id")] = page
        for page in all_pages:
            if page.METHOD_NAME is BillInitiator.METHOD_NAME:
                yield FilteredBill(page.all_field_values() + bills[page["bill_id"]].all_field_values())

    @classmethod
    def get_all_by_knesset_number(cls, knesset_num):
        params = {'$filter': f"KnessetNum eq {knesset_num}", '$expand': 'KNS_BillInitiators'}
        all_pages = cls._get_all_pages(cls._get_url_base(), params)
        return cls._flatten_pages(all_pages)


def write_csv(file_name, data):
    with open(f'{file_name}.csv', mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].all_field_values(), dialect='excel')
        writer.writeheader()
        for row in data:
            writer.writerow(row.all_field_values())
    pass


def get_person_ids_list(data):
    for r in data:
        yield r.person_id


def enrich_with_person_data(person_ids_list):
    for id in person_ids_list:
        yield Person.get(id)


if __name__ == '__main__':
    if len(sys.argv) < 1:
        print("Usage: python gender_gov.py <knesset_num>")
        sys.exit(0)

    knesset_num = sys.argv[1]
    # print(f"Fetching data for knesset number {knesset_num}")
    # print("Getting persons and their positions...")
    # person_positions = list(FilteredPersonToPosition.get_all_by_knesset_number(knesset_num))
    # person_ids_set = set(get_person_ids_list(person_positions))
    # print("Done.")
    # print("Creating persons positions csv...")
    # write_csv(f"knesset_{knesset_num}_person_positions", person_positions)
    # print("Done.")
    # print("Getting persons data...")
    # person_data_list = list(enrich_with_person_data(person_ids_set))
    # print("Done.")
    # print("Creating persons data csv...")
    # write_csv(f"knesset_{knesset_num}_person_data", person_data_list)
    # print("Done.")
    # print("Creating bill initiators csv...")
    # write_csv(f"knesset_{knesset_num}_bill", list(FilteredBill.get_all_by_knesset_number(knesset_num)))
    # print("Done.")
    print("Creating committees...")
    write_csv(f"knesset_{knesset_num}_committees", list(Committee.get_all_active_committees()))
    print("Done.")
