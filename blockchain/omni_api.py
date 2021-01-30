import requests



# configs
end_points = {
        "v2_address":"https://api.omniwallet.org/v2/address/addr/",
        "v1_address":"https://api.omniexplorer.info/v1/address/addr/",
        "v1_address_details":"https://api.omniexplorer.info/v1/address/addr/details/",
        "v1_armory_getunsigned":"https://api.omniexplorer.info/v1/armory/getunsigned",
        "v1_armory_getransaction":"https://api.omniexplorer.info/v1/armory/getrawtransaction",
        "v1_decode":"https://api.omniexplorer.info/v1/decode/",
        "v1_omnidex_designatingcurrencies":"https://api.omniexplorer.info/v1/omnidex/designatingcurrencies",
        "v1_property_gethistory":"https://api.omniexplorer.info/v1/properties/gethistory/3",
        "v1_property_listbyowner":"https://api.omniexplorer.info/v1/properties/listbyowner",
        "v1_property_listbycrowedsales":"https://api.omniexplorer.info/v1/properties/listactivecrowdsales",
        "v1_property_listbyecosystem":"https://api.omniexplorer.info/v1/properties/listbyecosystem",
        "v1_property_list":"https://api.omniexplorer.info/v1/properties/list",
        "v1_transaction_address":"https://api.omniexplorer.info/v1/transaction/address",
        "v1_transaction_pushtx":"https://api.omniexplorer.info/v1/transaction/pushtx/",
        "v1_transaction_tx":"https://api.omniexplorer.info/v1/transaction/tx/",
        }

headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}


def base(end_point,data):
    response = requests.post(end_point, headers=headers, data=data)
    if response.status_code == 200:
        content = response.json()
        return(content)
    else:
        print(response.status_code, response.content)
 
def data_type_check(func):
    def check(x):
        if isinstance(x,str):
            return func(x)
        else:
            raise TypeError('Please input as string type variable')
    return check    

       
def v2_multi_address(addresses:list):
    if isinstance(addresses,list):
        data = []
        for add in addresses:
            data.append(('addr',add))
        end_point = end_points["v2_address"]
        content = base(end_point,data)
        return (content)
    else:
        raise TypeError('Please input as list type varible')

@data_type_check
def v1_single_address(address:str):
    data = {'addr':address} 
    end_point = end_points["v1_address"]
    content = base(end_point,data)
    return (content)

@data_type_check
def v1_address_details(address:str):
    data = {'addr':address} 
    end_point = end_points["v1_address_details"]
    content = base(end_point,data)
    return (content)

@data_type_check         
def v1_armory_getunsinged(unsigned_hex:str,pubkey:str):
    data = {'unsigned_hex':unsigned_hex,'pubkey':pubkey}
    end_point = end_points["v1_armory_getunsigned"]
    content = base(end_point,data)
    return content

@data_type_check
def v1_armory_getrawtx(armory_tx:str):
    data = {'unsigned_hex':armory_tx}
    end_point = end_points["v1_armory_getransaction"]
    content = base(end_point,data)
    return content

@data_type_check
def v1_decode(hex_:str):
    data = {'hex':hex_}
    end_point = end_points["v1_decode"]
    content = base(end_point,data)
    return content



@data_type_check        
def v1_omnidex_designatingcurrencies(ecosystem:str):
    data = {'ecosystem':ecosystem}
    end_point = end_points["v1_omnidex_designatingcurrencies"]
    content = base(end_point,data)
    return content


@data_type_check 
def v1_property_history(page:str):
    data = {'page':page}
    end_point = end_points["v1_property_gethistory"]
    content = base(end_point,data)
    return content

@data_type_check
def v1_property_listbyowner(address:str):
    data = {'addresses':address}
    end_point = end_points["v1_property_listbyowner"]
    content = base(end_point,data)
    return content


@data_type_check        
def v1_property_listbycrowedsales(ecosystem:str):
    data = {'ecosystem,':ecosystem}
    end_point = end_points["v1_property_listbycrowedsales"]
    content = base(end_point,data)
    return content

@data_type_check        
def v1_property_listbyecosystem(ecosystem:str):
    data = {'ecosystem':ecosystem}
    end_point = end_points["v1_property_listbyecosystem"]
    content = base(end_point,data)
    return content


def v1_property_list():
    data = {}
    end_point = end_points["v1_property_list"]
    content = base(end_point,data)
    return content
        
@data_type_check    
def v1_transaction_address(address:str):
    data = {'addr':address}
    end_point = end_points["v1_transaction_address"]
    content = base(end_point,data)
    return content
      
        

@data_type_check
def v1_transaction_pushtx(signed_tx:str):
    data = {'signedTransaction':signed_tx}
    end_point = end_points["v1_property_pushtx"]
    content = base(end_point,data)
    return content



@data_type_check          
def v1_transaction_tx(tx:str):
    end_point = end_points["v1_transaction_tx"] + tx
    content = requests.get(end_point)
    if content.status_code == 200:
        data = content.json()
        return data
    else:
        print(content.status_code,content['message'])

if __name__ == '__main__':
    data = [
            ('addr', '1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA'),
            ('addr', '1KYiKJEfdJtap9QX2v9BXJMpz2SfU4pgZw'),
           ]
    tx = "1" 
    test = v1_single_address('37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g')



