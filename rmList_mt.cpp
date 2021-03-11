
#ifndef UNICODE
  #define _UNICODE
  #define UNICODE
#endif

#pragma warning(push)
#pragma warning(disable:6385)

#include <errno.h>
#include <fstream>
#include <vector>
#include <iostream>
#include <string>
#include <stdio.h>
//#include <locale>
#include <cctype>
#include <assert.h>

#include <stdint.h>
#include <windows.h>
#include <strsafe.h>
#include <io.h>
#include <fcntl.h>

#include <chrono>


using namespace std;
chrono::high_resolution_clock::time_point start = chrono::high_resolution_clock::now();

size_t thMax = 10;  // max running threads at any time
unsigned int minFilesChunk = 1000, maxFilesChunk = 4000;
size_t chunckSz = 0, nbChunks;
HANDLE hMutex;
short thIdx = 0;
vector<string> fnames;  size_t nbFiles = 0;
vector<LPCWSTR> revisitDirs;
size_t *slots, *noFile, *miss, *delCnt;

inline void flushErr(LPCSTR format, ...);
inline void flushOut(LPCSTR format, ...);
inline char * wide2uf8(LPCWSTR);
inline wchar_t * uf8toWide(LPCCH);
inline LPSTR catStr(size_t firstArg, ...);
#define sysErr 10000+GetLastError()
inline void printErr(LPCSTR msg, LONG errCode, int exitCode=0);
inline void strCopy(LPWSTR dst, size_t sz, LPCWSTR src, bool truncate);
inline void checkSnprintfA(HRESULT,size_t);
#define snprintfA(dst,sz,fmt,...) checkSnprintfA(StringCchPrintfA(dst,sz,fmt, ##__VA_ARGS__),sz)
inline void formatDur(__int64 dur);
inline void checkAlloc(size_t nBlocs, UINT blocSz, ...);
void chunckLogic();
DWORD WINAPI deleteFiles(LPVOID p);

#define Lock() if (WAIT_FAILED == WaitForSingleObject(hMutex, INFINITE)) {\
    printErr("Error: system failure (wait).", sysErr); return 60; }
#define unLock() if (0 == ReleaseMutex(hMutex)) { printErr("Error: system failure (release).", sysErr); return 62; }

inline void trim(std::string &str){       if(str.empty()) return;
    const auto pStr = str.c_str();
    size_t front = 0, back = str.length();
    while(front < back && std::isspace(static_cast<unsigned char>(pStr[front]))) ++front;
    while(back > front && std::isspace(static_cast<unsigned char>(pStr[back-1]))) --back;

    if(0 == front){ if(back < str.length()) str.resize(back - front); return; } 
    if(back <= front) str.clear();
    else str = std::move( std::string(str.begin()+front, str.begin()+back) );
}

// Use as many threads as thMax allows, abiding by minFilesChunk ±20% / hard maxFilesChunk
void chunckLogic(){
  if(thMax == 1 || nbFiles <= thMax || nbFiles<=minFilesChunk){ chunckSz = nbFiles; nbChunks = thMax = 1; return; }
  chunckSz = nbFiles / thMax; 
  chunckSz = chunckSz>maxFilesChunk ? maxFilesChunk : chunckSz;
  chunckSz = chunckSz<minFilesChunk ? minFilesChunk : chunckSz;
  if(chunckSz >= nbFiles){ chunckSz = nbFiles; nbChunks = thMax = 1; return; }
  nbChunks = (-1+chunckSz+nbFiles) / chunckSz;
  if(nbChunks<=thMax) thMax = nbChunks;
  if(thMax==1) return;
  // distribute work load uniformly
  size_t uChunckSz = (-1+thMax+nbFiles) / thMax; if(uChunckSz>=nbChunks) return;
  if( uChunckSz < (8*minFilesChunk/10)){ thMax--; chunckLogic(); return; } //flushOut("\n#thMax = %lu\n", thMax); 
  else{ chunckSz = uChunckSz; nbChunks = (-1+chunckSz+nbFiles) / chunckSz; }
}

int main(int argc, char** argv) {
  
  setlocale ( LC_ALL, "en-US.65001" );  
  assert(thMax>0); assert(minFilesChunk>0); assert(maxFilesChunk>0);

  if (argc < 2) { fprintf(stderr, "\n  Error: missing argument: input file.\n\n"); exit(10); }

  int nbArgs;
  auto arglist = CommandLineToArgvW(GetCommandLineW(), &nbArgs);
  if(arglist==nullptr) printErr(nullptr, sysErr, 21);

  auto fn = wide2uf8(arglist[1]);
  ifstream inFile(fn, std::ios::in); // Read text  
  if (!inFile.is_open()) printErr(catStr(0,"\n  Error: couldn't open input file \"", fn, "\"", 0), sysErr, 33);
  inFile.exceptions(ifstream::badbit);
  string line; size_t ln = 0, invalid = 0, found, sz;
  try { while (getline(inFile, line)) {  ln++; trim(line); sz = line.size();
    if(line[0]=='"' && line[sz-1]=='"') line = std::move(std::string(line.begin()+1, line.begin()+sz-1));
    found = line.find_first_of("\"*<>");
    if(found!=string::npos){ invalid++; 
      flushErr("\nError: line #%lu: invalid path\n", ln); flushErr("  %s\n", line.c_str());
    flushErr("  %*.*s^ this character is illegal\n", found,found,""); }
    else{ trim(line); fnames.push_back(line); }
  }}
  catch (ifstream::failure e) { fprintf(stderr, "\nError reading file %s: %s\n", fn, e.what()); 
    inFile.close(); exit(5); }
  inFile.close(); 
  
  nbFiles = fnames.size();
  if (nbFiles == 0) { flushOut("\nEmpty file: %s\n\n", fn); ; exit(0); }
  delete[] fn;

  //HANDLE hFile = CreateFileW( L"titi",                // name of the write
     // GENERIC_WRITE,          // open for writing
     // 0,                      // do not share
     // NULL,                   // default security
     // CREATE_ALWAYS,             // create new file only
     // FILE_ATTRIBUTE_NORMAL,  // normal file
     // NULL);                  // no attr. template
  //wchar_t *fn = uf8toWide(fnames[0].c_str());
  //DWORD dwBytesToWrite = (DWORD)(sizeof(wchar_t)*wcslen(fn)), dwBytesWritten = 0;
  //if(FALSE == WriteFile(
     // hFile,           // open file handle
     // fn,      // start of data to write
     // dwBytesToWrite,  // number of bytes to write
     // &dwBytesWritten, // number of bytes that were written
     // NULL))
  //printErr("Error: WriteFile",sysErr);
  //flushOut("to write: %d, written: %d\n",dwBytesToWrite,dwBytesWritten);

  //thMax = 2; minFilesChunk = 1; maxFilesChunk = 10;
  chunckLogic(); 
  flushOut("\n#Files = %lu\n", nbFiles);
  if(thMax>1) flushOut("  %d chunk%s of %d file%s\n  -> Launching %lu thread%s\n",
    nbChunks, nbChunks==1?"":"s",     chunckSz, chunckSz==1?"":"s each",     thMax, thMax==1?"":"s");
  //exit(0);

  hMutex = CreateMutex(nullptr, FALSE, nullptr);
  if (nullptr == hMutex) printErr("Error: could not create mutex.", sysErr, 61);
  
  DWORD *dwThreadId = new DWORD[thMax], *dwExitCode = new DWORD[thMax]; checkAlloc(thMax, sizeof(DWORD), 2, dwThreadId, dwExitCode);
  HANDLE* hThread = new HANDLE[thMax]; checkAlloc(thMax, sizeof(HANDLE), 1, hThread);
  
  slots = new size_t[thMax](); checkAlloc(thMax, sizeof(size_t), 1, slots);
  delCnt = new size_t[nbChunks+1](); miss = new size_t[nbChunks+1](); 
  noFile = new size_t[nbChunks+1](); checkAlloc(nbChunks+1, sizeof(size_t), 3, delCnt, miss, noFile);
  size_t mis, totalMiss = 0, totalDel = 0, totalNoFile = 0, currChunck = 1; USHORT p = 0;
  auto fullthGroups = (-1+nbChunks+thMax)/thMax; size_t th2Launch;
  while(currChunck<=nbChunks) { fullthGroups--;
    th2Launch = thMax;
    if(fullthGroups==0) th2Launch = nbChunks%thMax; // last round
	
    for(p=0; p<th2Launch; p++) {  slots[p] = currChunck;
      hThread[p] = CreateThread(nullptr, 0, deleteFiles, &slots[p], 0, &dwThreadId[p]); currChunck++;
      if (nullptr == hThread[p]) printErr("Error: failed to create thread.", sysErr, 59);
    }
    DWORD done = WaitForMultipleObjects((DWORD)th2Launch, hThread, TRUE, INFINITE);
    if (WAIT_FAILED==done) printErr("Error: system failure (wait multi).", sysErr, 60);
	
	size_t u;
	for(p=0; p<th2Launch; p++) {
      GetExitCodeThread(hThread[p], &dwExitCode[p]);
      CloseHandle(hThread[p]);  u = slots[p];
      mis = miss[u]; totalMiss += mis; totalDel += delCnt[u]; totalNoFile += noFile[u];
      //if(thMax>1) { dwExitCode[p] = (mis!=0 && dwExitCode[p]==0) ? 1:dwExitCode[p];
      //  if(mis>0 || 0!=dwExitCode[p])
      //    flushOut("\nThread #%lu exit code = %lu (missed %lu file%s)\n", u, dwExitCode[p], mis, mis==1?"":"s");
      //}
    }
  }
  fnames = vector<string>(); delete[] slots; delete[] delCnt; delete[] miss; delete[] noFile;

  LPCWSTR fe; DWORD errorID; char* fu = nullptr;
  for(size_t i=0;i<revisitDirs.size();i++){ fe = revisitDirs[i];
    if(0==RemoveDirectoryW(fe)){ errorID = GetLastError(); totalMiss++; fu = wide2uf8(fe);
	  flushErr("\nError deleting directory: %s\n", fu); printErr(nullptr, 10000+errorID); delete[] fu;
	} else totalDel++;
	delete[] fe;
  }; revisitDirs = vector<LPCWSTR>();

  flushErr("\n");
  if(invalid>0) flushErr("Ignored %lu invalid item%s (out of %lu)\n", invalid, invalid == 1 ? "" : "s", ln);
  if(totalMiss>0) flushErr("Failed to delete %lu item%s (out of %lu)\n", totalMiss, totalMiss == 1 ? "" : "s", nbFiles);
  if(totalNoFile == 1) flushErr("The system could not find 1 item\n");
  else if(totalNoFile > 1){ flushErr("The system could not find %lu items", totalNoFile);
     if(ln!=totalNoFile) flushErr(" (out of %lu)\n", nbFiles);
     else flushErr(" (out of %lu)\n", nbFiles);
  }
  if(totalDel==0) flushErr("Deleted nothing\n");
  else flushErr("Deleted %lu item%s\n", totalDel, totalDel==1 ? "":"s");

  //_sleep(1500);
  chrono::high_resolution_clock::time_point end = chrono::high_resolution_clock::now();
  formatDur( chrono::duration_cast<chrono::microseconds>(end - start).count() );
  return 0;
}


DWORD WINAPI deleteFiles(LPVOID p) {
  size_t chunck = *(size_t *) p;
  size_t s, deleted = 0;
  s = chunckSz*(chunck-1);
  auto e = s -1+chunckSz; if(e>=nbFiles) e = nbFiles - 1;
  //Lock(); flushErr("Thread %lu running: %lu to %lu\n", chunck, 1+s, 1+e); unLock();
  size_t mis = 0, DIR=0, notfound = 0; LPCWSTR fn; DWORD errorID, attr;
  for(auto i=s; i<=e; i++){ 
    //if(remove(fnames[i].c_str()) != 0){ mis++; Lock(); perror("Error deleting file"); unLock(); }
    fn = uf8toWide(fnames[i].c_str()); //wFn[chunck-1+i] = 
    if(0==DeleteFileW(fn)) {      errorID = GetLastError();
      if(errorID==ERROR_FILE_NOT_FOUND||errorID==ERROR_PATH_NOT_FOUND){ delete[] fn; notfound++; continue; }
      mis++;
      attr = GetFileAttributesW(fn);
      if(INVALID_FILE_ATTRIBUTES!=attr && attr&FILE_ATTRIBUTE_DIRECTORY){
        if(0==RemoveDirectoryW(fn)){  errorID = GetLastError();
		  if(errorID==ERROR_DIR_NOT_EMPTY){ revisitDirs.push_back(fn); DIR=1; mis--; }
		  else{ Lock(); flushErr("\nError deleting directory: %s\n", fnames[i].c_str()); printErr(nullptr, 10000+errorID); unLock(); }
		}
		else{ mis--; deleted++; }
      }
      else { Lock(); flushErr("\nError deleting: %s\n", fnames[i].c_str()); printErr(nullptr, 10000+errorID); unLock(); }
    }
	else deleted++; 
    if(DIR==0) delete[] fn;
  }
  miss[chunck] = mis; noFile[chunck] = notfound; delCnt[chunck] = deleted;
  return 0;
}

inline void checkAlloc(size_t nBlocs, UINT blocSz, ...) {
    va_list args;
    va_start(args, blocSz);
    auto count = va_arg(args, USHORT);
    for(USHORT i = 0; ++i <= count;) if (nullptr == va_arg(args, void*)) {
        fprintf(stderr, "\n  Error: not enough memory (trying to reserve %llux%d bytes).\n\n", nBlocs, blocSz); exit(14);
    }
    va_end(args);
}

inline void formatDur(__int64 dur) {
    size_t s = (size_t)(dur / 1000000); UINT ms = (UINT)(dur / 1000 - 1000 * (dur / 1000000));
    UINT h = (UINT)(s / 3600); UINT m = (UINT)((s - 3600 * h) / 60);
    //flushOut("\nDone in %02lu:%02lu:%02lu.%lums\n\n", h, m, s, ms);

    size_t n = 65, cnt = 0;
    CHAR* sh = new CHAR[n](), * sm = new CHAR[n](), * ss = new CHAR[n](); checkAlloc(n, sizeof(CHAR), 3, sh, sm, ss);
    flushOut("Done in ");
    //if (d > 0) { snprintf(sd, 100, "%ld day", d);    cnt++; }; if (d > 1 || d == 0)  strcat(sd, "s");
    //h = 2; m = 14; s = 23;
    if(h>0){ snprintfA(sh, n, "%d hour", 2);   cnt++; }; if (h != 1)  strcat(sh, "s");
    if(m>0){ snprintfA(sm, n, "%d minute", m); cnt++; }; if (m != 1)  strcat(sm, "s");
    if(s>0){ snprintfA(ss, n, " second");      cnt++; }; if (ms>0 || s!=1) strcat(ss, "s");
    if(cnt==0){ flushOut("%d millisecond", ms); if (ms > 1) flushOut("s\n\n"); else flushOut("\n\n"); return; }
    //if(d>0){ cnt--; flushOut("%s", sd); if (cnt > 0) flushOut(" and "); else flushOut(", "); }
    if(h>0){ cnt--; flushOut("%s", sh); if (cnt==0) flushOut(" and "); else flushOut(", "); }
    if(m>0){ cnt--; flushOut("%s", sm); if (cnt==0) flushOut(" and "); else flushOut(", "); }
    if(s>0){ flushOut("%lu.%03d", s, ms); flushOut("%s", ss); }
    else{ flushOut("%d millisecond", ms); if (ms != 1) flushOut("s"); }
    flushOut("\n\n");
}

inline wchar_t * uf8toWide(LPCCH str) {
    auto dwCount = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, str, -1, nullptr, 0);
    if (0==dwCount){ DWORD errorMessageID = GetLastError();
      fprintf(stderr, "Error: MultiByteToWideChar() failed: %s\n",str); fflush(stderr);
      printErr(nullptr, 10000+errorMessageID, 59); }
    wchar_t *pText = new wchar_t[dwCount](); checkAlloc(dwCount, sizeof(wchar_t), 1, pText);
    if(0==MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, str, -1, pText, dwCount))
        printErr("Error: uf8toWide(): MultiByteToWideChar() failed\n", sysErr, 55);
    return pText;
}

inline char * wide2uf8(LPCWSTR str) {
  auto dwCount = WideCharToMultiByte(CP_UTF8, 0, str, -1, nullptr, 0, nullptr, nullptr);
  if(0==dwCount){ DWORD errorMessageID = GetLastError();
    fprintf(stderr, "Error: wide2uf8(): WideCharToMultiByte() failed.\n"); fflush(stderr); 
    printErr(nullptr, 10000+errorMessageID, 57); }
  char *pText = new char[dwCount](); checkAlloc(dwCount, sizeof(char), 1, pText);
  if(0==WideCharToMultiByte(CP_UTF8, 0, str, -1, pText, dwCount, nullptr, nullptr)) {  DWORD errorMessageID = GetLastError();
    fprintf(stderr, "Error: wide2uf8(): WideCharToMultiByte() failed\n"); fflush(stderr);
    LPSTR messageBuffer = nullptr;
    size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr,
      errorMessageID,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPSTR)&messageBuffer, 0, nullptr);
    if(size>0){ fprintf(stderr, "(Err %lu) %s\n", errorMessageID, messageBuffer); fflush(stderr); }
    exit(58);
  }
  return pText;
}


inline void flushErr(LPCSTR format, ...) {
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args); fflush(stderr);
  va_end(args);
}
inline void flushOut(LPCSTR format, ...) {
  va_list args;
  va_start(args, format);
  vfprintf(stdout, format, args); fflush(stdout);
  va_end(args);
}

inline void printErr(LPCSTR msg, LONG errCode, int exitCode) {
  if (errCode > 10000) {  // query system
    errCode -= 10000;
    if(nullptr != msg) flushErr("%s\n", msg);
    LPWSTR messageBuffer = nullptr;
    if(0 < FormatMessageW(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr, (DWORD) errCode, 0, (LPWSTR)&messageBuffer, 0, nullptr)){
    LPSTR u8str = wide2uf8(messageBuffer); 
	size_t n = strlen(u8str) - 1; while(u8str[n]=='\n') u8str[(n--)] = 0;  // thank you
	flushErr("  (Err %d) %s\n", errCode, u8str); 
    LocalFree(messageBuffer); delete[] u8str; //free((void *)u8str);
    }
    if(exitCode!=0) exit(exitCode);
    return;
  }
  if(nullptr != msg) flushErr("%s\n", msg);
  if(exitCode!=0) exit(exitCode);
}

inline void strCopy(LPWSTR dst, size_t sz, LPCWSTR src, bool truncate=TRUE) {
    auto hRslt = StringCchCopy(dst, sz, src);
    if(hRslt==STRSAFE_E_INVALID_PARAMETER || (!truncate && hRslt==STRSAFE_E_INSUFFICIENT_BUFFER)) {
        fprintf(stderr, "Error: StringCchCopy() failed.\n"); exit(16);
    }
}

inline LPSTR catStr(size_t firstArg, ...) {
    size_t st = 0, len;
    LPSTR str, msg = new CHAR[1](); checkAlloc(1, sizeof(CHAR), 1, msg);
    HRESULT hRslt = S_OK;
    va_list args;
    va_start(args, firstArg);
    while(nullptr != (str = va_arg(args, LPSTR)) && 0 != lstrcmpA(str, "")) {
        st += 1 + (len = lstrlenA(str));
        checkAlloc(st, sizeof(CHAR), 1, (msg = (LPSTR)realloc((void *)msg, st * sizeof(CHAR))));
        hRslt = StringCchCatA(msg, st, str);
        if(FAILED(hRslt)) {
            fprintf(stderr, "Error: catStr(): StringCchCatW() failed: ");
            if(hRslt==STRSAFE_E_INSUFFICIENT_BUFFER) fprintf(stderr, "INSUFFICIENT_BUFFER of %llu bytes\n", st * sizeof(CHAR));
            else fprintf(stderr, "INVALID_PARAMETER: %llu chars not between 0 and %d\n", st, STRSAFE_MAX_CCH);
            exit(16);
        }
    }
    va_end(args);
    return msg;
}

inline void checkSnprintfA(HRESULT hRslt, size_t sz) {
    if(FAILED(hRslt)) {
        fprintf(stderr, "Error: snprintfA(): StringCchPrintfA() failed: ");
        if (hRslt==STRSAFE_E_INSUFFICIENT_BUFFER) fprintf(stderr, "INSUFFICIENT_BUFFER of %llu bytes\n", sz * sizeof(CHAR));
        else fprintf(stderr, "INVALID_PARAMETER: %llu chars not between 0 and %d\n", sz, STRSAFE_MAX_CCH);
        exit(18);
    }
    return;
}



