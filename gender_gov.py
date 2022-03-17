import csv
import sys

from knesset_data.dataservice.bills import Bill
from knesset_data.dataservice.persons import Person, PersonToPosition


class FilteredPerson(Person):
    @classmethod
    def get_all_present_members(cls, proxies=None):
        params = {'$filter': 'IsCurrent eq true'}
        return cls._get_all_pages(cls._get_url_base(), params=params, proxies=proxies)


class FilteredPersonToPosition(PersonToPosition):
    @classmethod
    def get_all_by_knesset_number(cls, knesset_num):
        params = {'$filter': f"KnessetNum eq {knesset_num}"}
        return cls._get_all_pages(cls._get_url_base(), params=params)


class FilteredBill(Bill):
    @classmethod
    def get_all_by_knesset_number(cls, knesset_num):
        params = {'$filter': f"KnessetNum eq {knesset_num}"}
        return cls._get_all_pages(cls._get_url_base(), params)


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
    knesset_num = sys.argv[1]
    person_positions = list(FilteredPersonToPosition.get_all_by_knesset_number(knesset_num))
    person_ids_set = set(get_person_ids_list(person_positions))

    write_csv(f"knesset_{knesset_num}_person_positions", person_positions)

    person_data_list = list(enrich_with_person_data(person_ids_set))

    write_csv(f"knesset_{knesset_num}_person_data", person_data_list)
    write_csv(f"knesset_{knesset_num}_bill", list(FilteredBill.get_all_by_knesset_number(knesset_num)))

