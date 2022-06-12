#include <iostream>
#include <string>
#include <fstream>
using namespace std;
int main(){
	ofstream f("select.sql");
	for(int i =0;i<10000;i++){
		f << "select * from person where pid = " << i << ";" << endl;
	}
	return 0;
}
