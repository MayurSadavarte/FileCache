#include"file_cache.h"
#include"file_cache_impl.h"
#include<stdio.h>
#include<string.h>
#include<stdlib.h>

int main() {
	unsigned int max_cache_entries = 5;
	FileCache *fc = fileCacheFactory( max_cache_entries );	
	std::vector<std::string> fileNames;
	fileNames.push_back("One");
	fileNames.push_back("Two");
	fileNames.push_back("Three");
	
	fc->PinFiles( fileNames );

	fileNames.pop_back();
	fc->UnpinFiles( fileNames );

	fileNames.pop_back();
	fileNames.pop_back();

	fileNames.push_back("Two");
	fileNames.push_back("Three");
	fileNames.push_back("Four");
	fileNames.push_back("Five");
	fileNames.push_back("Six");
	fc->PinFiles( fileNames );

	char *testStr = "Nutanix File Cache Test";
	char *wbuffer = fc->MutableFileData("Four");
	memcpy( wbuffer, testStr, strlen(testStr)+1 );

	fileNames.clear();
	fileNames.push_back("Four");
	fc->UnpinFiles( fileNames );

	fileNames.clear();
	fileNames.push_back("One");
	fc->PinFiles( fileNames );
	fc->UnpinFiles( fileNames );

	fileNames.clear();
	fileNames.push_back("Four");
	fc->PinFiles( fileNames );

	const char *rbuffer = fc->FileData("Four");
	printf("\n%s\n", rbuffer );

	delete fc;	

	return 0;
}
