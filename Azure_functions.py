import os
from azure.storage.blob import ContainerClient
from IPython.display import clear_output
connection = 'https://*****??testcontainer****'
class Azure_functions:

    def __init__(self,connection):
        self.client = ContainerClient.from_container_url(connection)

    def list_function_hierrchical(self,prefix):
        for blob in self.client.walk_blobs(name_starts_with=prefix + '/', delimiter='/'):
            print(blob.name)
                                           
    def check_function(self):
        check = input("Do you want to check again ?(y/n)").casefold()
        if  check  == 'y':
            return self.main_menu()
        elif check  == 'n':
            print('Thank you!!')
        else:
            return self.check_function()
        
    def data_upload_function(self,Data,file_name="sample_file"):
        try:
            if __name__ == "__main__":
                print("Pass")
            container_client = self.client.get_blob_client(blob=f'path/folder/sub-folder/sample_file.csv')
            if __name__ == "__main__":
                print("Pass")
            container_client.upload_blob(Data,blob_type='BlockBlob')
        except:
            container_client.delete_blob()
            return self.data_upload_function(Data,file_name="sample_file")

    def main_menu(self):
        clear_output(wait=True)
        d= {}
        count = 0
        for index,x in enumerate(self.client.list_blob_names()):
            if x.count('/') <= 4:
                count += 1
                print(count,'',x)
                d[count] = x
        num = int(input())
        if num > count:
            return self.main_menu()
        else:
            surfix = d[num]
            print('-'*90)
            self.list_function_hierrchical(surfix)
            print('-'*90)
            return self.check_function()
        
    def delete_function(self):
        d= {}
        count = 0
        for index,x in enumerate(self.client.list_blob_names()):
            if x.count('/') > 4:
                count += 1
                print(count,'',x)
                d[count] = x
        num = int(input())
        surfix = d[num]
        print('-'*90)
        print('Deleting the file below')
        container_client = self.client.delete_blobs(surfix)
        print('-'*90)
        return container_client, print(f'{surfix}')
    
    def Download_function_1(self,download_file_path):
        ls = []
        for x in self.client.list_blobs(name_starts_with=download_file_path):
            ls.append(x.name)
        for l in ls:
            if ".csv" in l:        
                os.makedirs(os.getcwd() + "\\{}\\{}".format(l.split('/')[-3],l.split('/')[-2]),exist_ok = True)
                path_1 = os.getcwd() + "\\{}\\{}".format(l.split('/')[-3],l.split('/')[-2])
                if __name__ == "__main__":
                    print(path_1)
                filename = l.split('/')[-3] + '_' + l.split('/')[-2]+'_'+ l.split('/')[-1]
                with open(file=os.path.join(path_1,filename),mode="wb") as sample_blob:
                     sample_blob.write(self.client.download_blob(l).readall())
        return print("complete")
    
    def Download_menu(self,s=3):
        d= {}
        count = 0
        clear_output(wait=False)
        for index,x in enumerate(self.client.list_blob_names()):
              if x.count('/') < s:
                count += 1
                print(index,'',x)
                d[index] = x
        ask = (input("Download - D Next - N Back - B")).casefold()
        if ask == 'd':
            num = int(input('Select the corresponding index'))
            surfix = d[num]
            return self.Download_function_1(surfix)
        if ask == 'n':
            return self.Download_menu(s+1)
        elif ask == 'b':
            return self.Download_menu(s-1)
        else:
            return print('INVALID INPUT'), self.Download_menu(s=3)
# Create object of the class
d = Azure_functions(connection)
