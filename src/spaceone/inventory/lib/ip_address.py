# -*- coding: utf-8 -*-
import ipaddress


class IPAddress(object):

    @staticmethod
    def get_ip_object(ip_address):
        return ipaddress.ip_address(ip_address)

    @staticmethod
    def get_network_object(network):
        return ipaddress.ip_network(network)

    @staticmethod
    def check_valid_ip(ip_address):
        try:
            ip = ipaddress.ip_address(ip_address)
            return str(ip)
        except ValueError:
            return False

    @staticmethod
    def check_valid_network(cidr):
        try:
            network = ipaddress.ip_network(cidr)
            return str(network)
        except ValueError:
            return False

    @staticmethod
    def check_duplicate_cidr_range(cidr1, cidr2):
        """
        if CIDRs was duplicated, return True
        """
        try:
            c1 = ipaddress.ip_network(cidr1)
            c2 = ipaddress.ip_network(cidr2)

            if c1.subnet_of(c2) or c1.supernet_of(c2):
                return True

            return False

        except ValueError:
            return False

    @staticmethod
    def check_subnet_of_network(subnet_cidr, net_cidr):
        subnet_cidr = ipaddress.ip_network(subnet_cidr)
        net_cidr = ipaddress.ip_network(net_cidr)

        return subnet_cidr.subnet_of(net_cidr)

    @staticmethod
    def check_valid_ip_in_network(ip, cidr):
        _ip = ipaddress.ip_address(ip)
        _net = ipaddress.ip_network(cidr)

        if _ip in _net:
            return True

        return False