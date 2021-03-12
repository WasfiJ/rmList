
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
size_t chunkSz = 0, nbChunks;
HANDLE hMutex;
UINT thIdx = 0;
vector<string> fnames;  size_t nbFiles = 0;
vector<LPCWSTR> revisitDirs;
vector<bool> slots;
vector<size_t> noFile, miss, delCnt;

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
void chunkLogic();
void runThreads(HANDLE*& hThread, vector<DWORD>& dwExitCode, vector<DWORD>& dwThreadId);
DWORD WINAPI deleteFiles(LPVOID p);
template <typename T> void checkedRealloc(T*& x, size_t count);

void checkedAlloc(size_t count) {}

template <typename T, class ... Ts>
void checkedAlloc(size_t count, T*& x, Ts&& ...args) {
  try { x = new T[count](); }
  catch (const std::bad_alloc) {
    fprintf(stderr, "Error allocating memory (%zu x %zu bytes).\n\n", count, sizeof(T)); 
    exit(15);
  }
  checkedAlloc(count, args...);
}

void checkedVectResz(size_t count) {}

template <typename T, class ... Ts>
void checkedVectResz(size_t count, vector<T>& x, Ts&& ...args) {
  try { x.resize(count); }
  catch (const std::bad_alloc) {
    fprintf(stderr, "Error allocating memory (%zu x %zu bytes).\n\n", count, sizeof(T)); 
    exit(14);
  }
  checkedVectResz(count, args...);
}

#define Lock() if (WAIT_FAILED == WaitForSingleObject(hMutex, INFINITE)) {\
  printErr("Error: system failure (wait).", sysErr); return 60; }
#define unLock() if (0 == ReleaseMutex(hMutex)) { printErr("Error: system failure (release).", sysErr); return 62; }

inline void trim(std::string &str){     if(str.empty()) return;
  const auto pStr = str.c_str();
  size_t front = 0, back = str.length();
  while(front < back && std::isspace(static_cast<unsigned char>(pStr[front]))) ++front;
  while(back > front && std::isspace(static_cast<unsigned char>(pStr[back-1]))) --back;

  if(0 == front){ if(back < str.length()) str.resize(back - front); return; } 
  if(back <= front) str.clear();
  else str = std::move( std::string(str.begin()+front, str.begin()+back) );
}

// Use as many threads as thMax allows, abiding by minFilesChunk ±20% / hard maxFilesChunk
void chunkLogic(){
  if(thMax == 1 || nbFiles <= thMax || nbFiles<=minFilesChunk){ chunkSz = nbFiles; nbChunks = thMax = 1; return; }
  chunkSz = nbFiles / thMax; 
  chunkSz = chunkSz>maxFilesChunk ? maxFilesChunk : chunkSz;
  chunkSz = chunkSz<minFilesChunk ? minFilesChunk : chunkSz;
  if(chunkSz >= nbFiles){ chunkSz = nbFiles; nbChunks = thMax = 1; return; }
  nbChunks = (-1+chunkSz+nbFiles) / chunkSz;
  if(nbChunks<=thMax) thMax = nbChunks;
  if(thMax==1) return;
  // distribute work load uniformly
  size_t uChunkSz = (-1+thMax+nbFiles) / thMax; if(uChunkSz>=nbChunks) return;
  if( uChunkSz < (8*minFilesChunk/10)){ thMax--; chunkLogic(); return; } //flushOut("\n#thMax = %lu\n", thMax); 
  else{ chunkSz = uChunkSz; nbChunks = (-1+chunkSz+nbFiles) / chunkSz; }
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
  try {
    while (getline(inFile, line)) {  ln++; trim(line); sz = line.size();
      if(line[0]=='"' && line[sz-1]=='"') line = std::move(std::string(line.begin()+1, line.begin()+sz-1));
      found = line.find_first_of("\"*<>");
      if(found!=string::npos){     invalid++; 
        flushErr("\nError: line #%lu: invalid path\n", ln); flushErr("  %s\n", line.c_str());
        flushErr("  %*.*s^ this character is illegal\n", found,found,""); 
      }
      else{ trim(line); fnames.push_back(line); }
    }
  }
  catch (ifstream::failure e) { fprintf(stderr, "\nError reading file %s: %s\n", fn, e.what()); inFile.close(); exit(5); }
  inFile.close(); 
  
  nbFiles = fnames.size();
  if (nbFiles == 0) { flushOut("\nEmpty file: %s\n\n", fn); ; exit(0); }
  delete[] fn;

  chunkLogic(); 
  flushOut("\n#Files = %lu\n", nbFiles);
  if(thMax>1) flushOut("  %d chunk%s of %d file%s\n  -> Launching %lu thread%s\n",
    nbChunks, nbChunks==1?"":"s",   chunkSz, chunkSz==1?"":"s each",   thMax, thMax==1?"":"s");

  vector<DWORD> dwThreadId, dwExitCode;
  checkedVectResz(thMax,      dwThreadId, dwExitCode);
  checkedVectResz(nbChunks+1, delCnt, noFile, slots, miss);

  HANDLE* hThread = nullptr; checkedAlloc(thMax, hThread); 

  hMutex = CreateMutex(nullptr, FALSE, nullptr);
  if (nullptr == hMutex) printErr("Error: could not create mutex.", sysErr, 61);

  size_t mis = 0, totalMiss = 0, totalDel = 0, totalNoFile = 0;
  runThreads(hThread, dwExitCode, dwThreadId);

  for(size_t p=1; p<=nbChunks; p++) {
    mis = miss[p]; totalMiss += mis; totalDel += delCnt[p]; totalNoFile += noFile[p];
  }

  fnames = vector<string>(); slots = vector<bool>(); delCnt = vector<size_t>(); 
  miss = vector<size_t>(); noFile = vector<size_t>();

  DWORD errorID; char* fu = nullptr;
  for(auto fe : revisitDirs) {
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

void runThreads(HANDLE*& hThread, vector<DWORD>& dwExitCode, vector<DWORD>& dwThreadId){
  size_t p;
  for(p=0; p<thMax; p++) { 
    hThread[p] = CreateThread(nullptr, 0, deleteFiles, nullptr, 0, &dwThreadId[p]);
    if (nullptr == hThread[p]) printErr("Error: failed to create thread.", sysErr, 59);
  }
  if (WAIT_FAILED==WaitForMultipleObjects((DWORD)thMax, hThread, TRUE, INFINITE)) 
    printErr("Error: system failure (wait multi).", sysErr, 60);

  for(p=0; p<thMax; p++) { GetExitCodeThread(hThread[p], &dwExitCode[p]);  CloseHandle(hThread[p]); }
}

DWORD chewChunk(size_t chunk);

DWORD WINAPI deleteFiles(LPVOID p) {
  UINT thId = 0, err; //GetCurrentThreadId();
  size_t k; bool gotSlot;
  while(true) {  // while there are chunks to consume
    gotSlot = false;
    Lock();
      for (k = 0; k < nbChunks; k++)
        if (!slots[k]){ slots[k] = gotSlot = true; break; }  // slot/chunk k is taken now
      if (thId == 0) thId = (UINT) ++thIdx;
    unLock(); 

    if (!gotSlot) return 0; // no-mo-chunks
    err = chewChunk(k+1); if(err!=0) return err;
  }
}

DWORD chewChunk(size_t chunk){
  size_t s = chunkSz*(chunk-1), deleted = 0;
  auto e = s -1+chunkSz; if(e>=nbFiles) e = nbFiles - 1;
  size_t mis = 0, DIR=0, notfound = 0; LPCWSTR fn; DWORD errorID, attr;
  for(auto i=s; i<=e; i++){ 
    fn = uf8toWide(fnames[i].c_str());
    if(0==DeleteFileW(fn)) {    errorID = GetLastError();
      if(errorID==ERROR_FILE_NOT_FOUND||errorID==ERROR_PATH_NOT_FOUND){ delete[] fn; notfound++; continue; }
      mis++;
      attr = GetFileAttributesW(fn);
      if(INVALID_FILE_ATTRIBUTES!=attr && attr&FILE_ATTRIBUTE_DIRECTORY){
        if(0==RemoveDirectoryW(fn)){  errorID = GetLastError();
          if(errorID==ERROR_DIR_NOT_EMPTY){ revisitDirs.push_back(fn); DIR=1; mis--; }
          else{    Lock(); 
            flushErr("\nError deleting directory: %s\n", fnames[i].c_str()); printErr(nullptr, 10000+errorID); unLock(); 
          }
        }
        else{ mis--; deleted++; }
      }
      else { Lock(); flushErr("\nError deleting: %s\n", fnames[i].c_str()); printErr(nullptr, 10000+errorID); unLock(); }
    }
    else deleted++; 
    if(DIR==0) delete[] fn;
  }
  miss[chunk] = mis; noFile[chunk] = notfound; delCnt[chunk] = deleted;
  return 0;
}

template <typename T>
void checkedRealloc(T*& x, size_t count) {
  x = (T*) realloc((void *)x, count * sizeof(T));
  if(nullptr == x){
    fprintf(stderr, "\n  Error1: not enough memory (trying to reserve %zu x %zu bytes).\n\n", count, sizeof(T)); exit(14);
  }
}

inline void formatDur(__int64 dur) {
  size_t s = (size_t)(dur / 1000000); UINT ms = (UINT)(dur / 1000 - 1000 * (dur / 1000000));
  UINT h = (UINT)(s / 3600); UINT m = (UINT)((s - 3600 * h) / 60);
  //flushOut("\nDone in %02lu:%02lu:%02lu.%lums\n\n", h, m, s, ms);

  size_t n = 65, cnt = 0;  
  CHAR* sh = nullptr, *sm = nullptr, *ss = nullptr; checkedAlloc(n,sh,sm,ss);
  flushOut("Done in ");
  //if (d > 0) { snprintf(sd, 100, "%ld day", d);  cnt++; }; if (d > 1 || d == 0)  strcat(sd, "s");
  //h = 2; m = 14; s = 23;
  if(h>0){ snprintfA(sh, n, "%d hour", 2);   cnt++; }; if (h != 1)  strcat(sh, "s");
  if(m>0){ snprintfA(sm, n, "%d minute", m); cnt++; }; if (m != 1)  strcat(sm, "s");
  if(s>0){ snprintfA(ss, n, " second");    cnt++; }; if (ms>0 || s!=1) strcat(ss, "s");
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
  wchar_t *pText = nullptr; checkedAlloc(dwCount, pText);
  if(0==MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, str, -1, pText, dwCount))
    printErr("Error: uf8toWide(): MultiByteToWideChar() failed\n", sysErr, 55);
  return pText;
}

inline char * wide2uf8(LPCWSTR str) {
  auto dwCount = WideCharToMultiByte(CP_UTF8, 0, str, -1, nullptr, 0, nullptr, nullptr);
  if(0==dwCount){ DWORD errorMessageID = GetLastError();
  fprintf(stderr, "Error: wide2uf8(): WideCharToMultiByte() failed.\n"); fflush(stderr); 
  printErr(nullptr, 10000+errorMessageID, 57); }
  char *pText = nullptr; checkedAlloc(dwCount, pText);
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
  size_t st = 0, len = 0;
  LPSTR str = nullptr, msg = nullptr; checkedAlloc(1,msg);
  HRESULT hRslt = S_OK;
  va_list args;
  va_start(args, firstArg);
  while(nullptr != (str = va_arg(args, LPSTR)) && 0 != lstrcmpA(str, "")) {
    st += 1 + (len = lstrlenA(str));
	checkedRealloc(msg, st);
    hRslt = StringCchCatA(msg, st, str);
    if(FAILED(hRslt)) {
      fprintf(stderr, "Error: catStr(): StringCchCatW() failed: ");
      if(hRslt==STRSAFE_E_INSUFFICIENT_BUFFER) fprintf(stderr, "INSUFFICIENT_BUFFER of %zu bytes\n", st * sizeof(CHAR));
      else fprintf(stderr, "INVALID_PARAMETER: %zu chars not between 0 and %d\n", st, STRSAFE_MAX_CCH);
      exit(16);
    }
  }
  va_end(args);
  return msg;
}

inline void checkSnprintfA(HRESULT hRslt, size_t sz) {
  if(FAILED(hRslt)) {
    fprintf(stderr, "Error: snprintfA(): StringCchPrintfA() failed: ");
    if (hRslt==STRSAFE_E_INSUFFICIENT_BUFFER) fprintf(stderr, "INSUFFICIENT_BUFFER of %zu bytes\n", sz * sizeof(CHAR));
    else fprintf(stderr, "INVALID_PARAMETER: %zu chars not between 0 and %d\n", sz, STRSAFE_MAX_CCH);
    exit(18);
  }
  return;
}

