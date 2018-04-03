#!/usr/bin/env python
import sys
import time
import datetime

from threading import Thread, Lock

from subprocess import Popen, PIPE

try:
    from subprocess import DEVNULL
    from queue import Queue, Empty
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'wb')
    from Queue import Queue, Empty

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent, \
DirCreatedEvent, FileCreatedEvent, DirModifiedEvent, FileModifiedEvent

import curses as crs
import re

ON_POSIX = 'posix' in sys.builtin_module_names

RemoteName = "MSUGateway"
RemotePath = "testpush/"
NMaxToRun = 3

IsDry = True

ignore_list = ["^\."] # Stop hidden files and directories getting pushed up
ignore_list_comp = [re.compile(i) for i in ignore_list]

class SFTPConnectionTimeout(Exception):
    pass
class RSyncFailureException(Exception):
    pass


class SCPPushEventHandler(FileSystemEventHandler):

    def __init__(self, LocalPath, RemoteSSHName, RemotePath):
        super(SCPPushEventHandler,self).__init__()
        self.LocalPath = LocalPath

        self.SFTPMkdirList = []
        self.SFTPTransferList = []

        self.mutex = Lock()

        self.NMkdirQueued = 0
        self.NCopyQueued = 0

    def AddSFTPCopyCommand(self,fpath):
        relpath = fpath[len(self.LocalPath):]
        #bla/blabla/hello/file.py -> 3
        splits = relpath.split("/")
        ndirstraversed = len(splits)-1
        dirpath = "/".join(splits[0:ndirstraversed])

        cmdset = (ndirstraversed, [])

        if ndirstraversed > 0:
            #cd to local dir
            cmdset[1].append("lcd %s" % dirpath)
            #cd to remote dir
            cmdset[1].append("cd %s" % dirpath)

        #copy file
        filename = relpath[len(dirpath):]
        if filename[0] == '/':
            filename = filename[1:]
        cmdset[1].append("put %s" % filename)

        if ndirstraversed > 0:
            rtnpath = ""
            for i in xrange(ndirstraversed):
                rtnpath += "../"

            #cd back (local)
            cmdset[1].append("lcd %s" % rtnpath)
            #cd back (remote)
            cmdset[1].append("cd %s" % rtnpath)

        self.SFTPTransferList.append(cmdset)

    def AddSFTPMkdirCommand(self,fpath):
        #bla/blabla/hello/ ->  #bla/blabla/hello
        if fpath[-1] == '/':
            fpath = fpath[:len(fpath)-2]

        relpath = fpath[len(self.LocalPath):]
        #bla/blabla/hello -> 2
        splits = relpath.split("/")
        ndirstraversed = len(splits)-1
        dirpath = "/".join(splits[0:ndirstraversed])

        cmdset = (ndirstraversed, [])

        if ndirstraversed > 0:
            #cd to remote dir
            cmdset[1].append("cd %s" % dirpath)

        #mkdir
        ndirname = relpath[len(dirpath):]
        if ndirname[0] == '/':
            ndirname = ndirname[1:]
        cmdset[1].append("mkdir %s" % ndirname)

        if ndirstraversed > 0:
            rtnpath = ""
            for i in xrange(ndirstraversed):
                rtnpath += "../"

            #cd back (remote)
            cmdset[1].append("cd %s" % rtnpath)

        self.SFTPMkdirList.append(cmdset)

    def on_created(self, event):
        filename = event.src_path.split("/")[-1]
        if filename[-1] == '/':
            filename = filename[:-1]

        count_match = reduce(lambda x, y: bool(x) or bool(y),
            [x.match(filename) for x in ignore_list_comp])
        if count_match:
            print "[INFO]: Ignoring file %s." % filename
            return

        if not event.is_directory:
            self.mutex.acquire()
            self.AddSFTPCopyCommand(event.src_path)
            self.mutex.release()
            self.NCopyQueued += 1
        else:
            self.mutex.acquire()
            self.AddSFTPMkdirCommand(event.src_path)
            self.mutex.release()
            self.NMkdirQueued += 1

    def on_modified(self, event):
        if not event.is_directory :
            filename = event.src_path.split("/")[-1]
            if filename[-1] == '/':
                filename = filename[:-1]

            count_match = reduce(lambda x, y: bool(x) or bool(y),
                [x.match(filename) for x in ignore_list_comp])
            if count_match:
                print "[INFO]: Ignoring file %s." % filename
                return
            self.mutex.acquire()
            self.AddSFTPCopyCommand(event.src_path)
            self.mutex.release()
            self.NCopyQueued += 1

class pySFTPPush():
    def __init__(self, LocalPath, RemoteSSHName, RemotePath, InitPullAll=False,
        ExtraRSyncOptions=""):
        self.LocalPath = LocalPath
        if self.LocalPath[-1] != '/':
            self.LocalPath = "%s/" % self.LocalPath
        self.RemoteSSHName = RemoteSSHName
        self.RemotePath = RemotePath
        if self.RemotePath[-1] != '/':
            self.RemotePath = "%s/" % self.RemotePath
        self.event_handler = None
        self.msgBuffer = []
        self.messageh = 0
        self.messagew = 0
        self.msgbox = None
        self.topbar = None

        self.NMkdirComplete = 0
        self.NCopyComplete = 0

        self.SFTPProcess = None
        self.ReaderThread = None
        self.ReaderQueue = None
        self.BlinkerRow = -1
        self.InitPullAll = InitPullAll
        self.ExtraRSyncOptions = ExtraRSyncOptions

    def PrintMessages(self):
        #Write messages
        if not self.msgbox is None:
            self.msgbox.clear()
            #Chuck old messages
            if len(self.msgBuffer) >= self.messageh:
                self.msgBuffer = \
                    self.msgBuffer[-self.messageh:]

            for i,msg in enumerate(self.msgBuffer):
                if len(msg) >= self.messagew:
                    msg = msg[:self.messagew-1]
                self.msgbox.addstr(i,0, msg[1].ljust(self.messagew-1),
                    crs.color_pair(msg[0]) )
            self.msgbox.refresh()
        else: # not using curses
            for i,msg in enumerate(self.msgBuffer):
                print msg[1]
            self.msgBuffer = []

    def RefreshTitleBar(self):
        self.topbar.clear()
        str = ["Monitoring: %s, Forwarding to %s:%s" % \
            (self.LocalPath, self.RemoteSSHName,self.RemotePath), ""]
        if len(str[0]) > self.messagew-2:
            strcpy = str[0]
            str[0] = strcpy[:self.messagew-3]
            str[1] = strcpy[self.messagew-3:]
            if len(str[1]) > self.messagew:
                str[1] = str[1][:self.messagew-3]

        self.topbar.addstr(0,2,str[0].ljust(self.messagew-3),crs.color_pair(1))
        self.topbar.addstr(1,2,str[1].ljust(self.messagew-3),
            crs.color_pair(1))

        str = "[SFTP]: mkdir commands processed: %s/%s | put commands "\
            "processed: %s/%s" \
            % (self.NMkdirComplete,self.event_handler.NMkdirQueued, \
               self.NCopyComplete,self.event_handler.NCopyQueued)
        self.topbar.addstr(2,2,str.ljust(self.messagew-3),crs.color_pair(1))

        for i in xrange(3):
            self.topbar.addstr(i,0,"*" if i == self.BlinkerRow else " ",
                crs.color_pair(4))
            self.topbar.addstr(i,1,"|",
                crs.color_pair(1))
        self.topbar.refresh()

    def CheckSFTPProcess(self):
        if not self.SFTPProcess.poll() is None:
            print "[ERROR]: SFTP process has finished. Exiting..."
            raise SFTPConnectionTimeout()

    def FlushSFTPProcessStdOut(self):
        self.CheckSFTPProcess()
        while True:
            try:
                while True:
                    line = self.ReaderQueue[1].get_nowait()
                    self.msgBuffer.append((3,"!- %s" % line[:-1]))
            except Empty:
                break

        while True:
            try:
                while True:
                    line = self.ReaderQueue[0].get_nowait()
                    self.AddSFTPMessageToBuffer(line)
            except Empty:
                break

    def WriteToSFTPProcess(self, cmd):
        self.CheckSFTPProcess()
        for ln in cmd:
            self.SFTPProcess.stdin.write("%s\n" % ln)
        self.SFTPProcess.stdin.flush()

    def WaitForConnection(self):
        self.CheckSFTPProcess()
        msg = "Connecting..."
        self.msgBuffer.append((2,msg))
        self.PrintMessages()
        nsecs = 0
        while True:
            if nsecs > 15:
                raise SFTPConnectionTimeout()
            time.sleep(1)
            if (not self.msgbox is None):
                self.msgBuffer[-1] = (2, msg + "[Wait loop %s/%s]" % (nsecs,15))
            else:
                self.msgBuffer.append((2, msg + "[Wait loop %s/%s]" % (nsecs,15)))
            self.PrintMessages()
            try:
                line = self.ReaderQueue[1].get_nowait()
            except Empty:
                nsecs += 1
                continue
            else:
                self.AddSFTPMessageToBuffer(line)
                break
        if (not self.msgbox is None):
            del self.msgBuffer[-2]
        self.PrintMessages()
        return True

    def AddSFTPMessageToBuffer(self, message):
        print message, message[:5], "|", message[6:]
        if message[:5] == "sftp>":
            if "mkdir" not in message:
                return
            message = message[6:]
            self.NMkdirComplete += 1
        if message[:9] == "Uploading":
            self.NCopyComplete += 1

        self.msgBuffer.append((2,"-- %s" % message[:-1]))

    def WaitForSFTPResponse(self,nmaxsecs=15):
        nsecs = 0
        while True:
            if nsecs > nmaxsecs:
                return False
            time.sleep(1)

            ResponseEmpty = True
            try:
                line = self.ReaderQueue[0].get_nowait()
                self.msgBuffer.append((3,"!- %s" % line[:-1]))
            except Empty:
                pass
            else:
                ResponseEmpty = False
            try:
                line = self.ReaderQueue[1].get_nowait()
                self.AddSFTPMessageToBuffer(line)
            except Empty:
                pass
            else:
                ResponseEmpty = False
            if ResponseEmpty:
                nsecs += 1
                continue
            else:
                break

        return True

    def StartWindow(self, stdscr):
        stdscr.clear()
        stdscr.refresh()

        self.messageh = crs.LINES - 5
        self.messagew = crs.COLS

        self.topbar = crs.newwin(3,self.messagew,0,0)
        self.RefreshTitleBar()
        self.msgbox = crs.newwin(self.messageh, self.messagew,4,0)

    def StartConnection(self):
        self.msgBuffer.append((2,"[INFO]: %s " % ["sftp", self.RemoteSSHName]))
        self.SFTPProcess = Popen(["sftp", self.RemoteSSHName],
            stdin=PIPE, stdout=PIPE, stderr=PIPE)

        def enqueue_output(out, queue):
            for line in iter(out.readline, b''):
                queue.put(line)
            out.close()

        self.ReaderQueue = [Queue(), Queue()]
        self.ReaderThread = [Thread(target=enqueue_output,
            args=(self.SFTPProcess.stdout, self.ReaderQueue[0])),
            Thread(target=enqueue_output,
                args=(self.SFTPProcess.stderr, self.ReaderQueue[1]))]
        self.ReaderThread[0].daemon = True
        self.ReaderThread[0].start()
        self.ReaderThread[1].daemon = True
        self.ReaderThread[1].start()

        if not self.WaitForConnection():
            print "[ERROR]: Failed to open sftp session to %s." \
                % self.RemoteSSHName
            return

        if self.RemotePath != "":
            self.WriteToSFTPProcess(["cd %s" % self.RemotePath,])
        if self.LocalPath != "" and self.LocalPath != "./":
            self.WriteToSFTPProcess(["lcd %s" % self.LocalPath,])

        self.WaitForSFTPResponse()

    def RunInitRSync(self):
        RSyncCmd = ["rsync", "-avz", "%s:%s" \
            % (self.RemoteSSHName, self.RemotePath), self.LocalPath]
        RSyncCmd.extend(self.ExtraRSyncOptions)
        self.msgBuffer.append((2,"[INFO]: %s " % " ".join(RSyncCmd)) )
        self.PrintMessages()

        RSyncProcess = Popen(RSyncCmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        def enqueue_output(out, queue):
            for line in iter(out.readline, b''):
                queue.put(line)
            out.close()

        ReaderQueue = [Queue(),Queue()]
        ReaderThread = [Thread(target=enqueue_output,
            args=(RSyncProcess.stdout, ReaderQueue[0])),
            Thread(target=enqueue_output,
                args=(RSyncProcess.stderr, ReaderQueue[1]))]
        ReaderThread[0].daemon = True
        ReaderThread[0].start()
        ReaderThread[1].daemon = True
        ReaderThread[1].start()

        while True:
            if not RSyncProcess.poll() is None:
                RSyncProcess.wait()
                self.msgBuffer.append(
                (2,"RSync process finished with return code %s" \
                    % RSyncProcess.returncode))
                ReaderThread[0].join()
                ReaderThread[1].join()
                return RSyncProcess.returncode == 0
            time.sleep(1)
            try:
                while True:
                    line = ReaderQueue[1].get_nowait()
                    self.msgBuffer.append((3,"== %s" % line[:-1]))
            except Empty:
                self.PrintMessages()
            try:
                while True:
                    line = ReaderQueue[0].get_nowait()
                    self.msgBuffer.append((2,"== %s" % line[:-1]))
            except Empty:
                self.PrintMessages()
        ReaderThread[0].join()
        ReaderThread[1].join()
        return False

    def ObserverLoop(self, stdscr):
        self.event_handler = SCPPushEventHandler(self.LocalPath,
            self.RemoteSSHName, self.RemotePath)
        observer = Observer()
        observer.schedule(self.event_handler, self.LocalPath, recursive=True)
        observer.start()

        if not stdscr is None:
            self.StartWindow(stdscr)
        else: # not using curses
            self.messageh = 48
            self.messagew = 80

        if self.InitPullAll:
            if not self.RunInitRSync():
                raise RSyncFailureException()

        self.StartConnection()
        self.PrintMessages()

        try:
            while True:
                self.BlinkerRow = (self.BlinkerRow + 1) % 3
                if (not self.topbar is None):
                    self.RefreshTitleBar()

                self.FlushSFTPProcessStdOut()
                self.PrintMessages()
                time.sleep(1)
                self.FlushSFTPProcessStdOut()
                self.PrintMessages()

                self.event_handler.mutex.acquire()

                for sftpmsg in sorted(self.event_handler.SFTPMkdirList, \
                    key=lambda cmdset: cmdset[0]):
                    self.WriteToSFTPProcess(sftpmsg[1])
                self.event_handler.SFTPMkdirList = []

                for sftpmsg in sorted(self.event_handler.SFTPTransferList, \
                    key=lambda cmdset: cmdset[0]):
                    self.WriteToSFTPProcess(sftpmsg[1])
                self.event_handler.SFTPTransferList = []

                self.event_handler.mutex.release()

        except KeyboardInterrupt:
            if(self.event_handler.mutex.locked()):
                self.event_handler.mutex.release()
            observer.stop()

        self.msgBuffer.append((2,"[INFO]: Ending SFTP process... ") )
        self.PrintMessages()
        self.WriteToSFTPProcess(["bye",])
        self.SFTPProcess.wait()
        self.msgBuffer.append((2,"[INFO]: Done! ") )
        self.msgBuffer.append((2,"[INFO]: Waiting on SFTP pipe reader threads to join... ") )
        self.PrintMessages()
        self.ReaderThread[0].join(3)
        self.ReaderThread[1].join(3)
        self.msgBuffer.append((2,"[INFO]: Done! ") )
        observer.join()
        self.PrintMessages()

    def __call__(self, stdscr):
        crs.init_pair(1, crs.COLOR_WHITE, crs.COLOR_GREEN)
        crs.init_pair(2, crs.COLOR_WHITE, crs.COLOR_BLACK)
        crs.init_pair(3, crs.COLOR_RED, crs.COLOR_BLACK)
        crs.init_pair(4, crs.COLOR_RED, crs.COLOR_GREEN)

        self.ObserverLoop(stdscr)

def Usage():
    print "[USAGE]: Use like %s LocalPathToMonitor RemoteSSHName "\
        "RemotePath [-i [--exclude .git ...]\n" % sys.argv[0]
    print "\t RemotePath: Relative to user home directory or absolute. "
    print "\t\tSFTP will not resolve ~ or environment variables."
    print "\t -i        : Begin by using rsync to pull down the entire "
    print "\t\t           remote. Arguments after the -i will be passed "
    print "\t\t           to rsync."

if __name__ == "__main__":
    if len(sys.argv) < 4:
        Usage()
        exit(1)

    evloop = None
    if len(sys.argv) == 4:
        evloop = pySFTPPush(sys.argv[1], sys.argv[2], sys.argv[3])
    elif sys.argv[4] == "-i":
        evloop = pySFTPPush(sys.argv[1], sys.argv[2], sys.argv[3], True,
            sys.argv[5:])
    else:
        Usage()
        exit(1)

    try:
        crs.wrapper(evloop)
    except SFTPConnectionTimeout:
        print "[ERROR]: SSH connection timed out and the SFTP process was lost."
    except RSyncFailureException:
        print "[ERROR]: Intial RSync failed. Cannot continue. "
        exit(1)

    if not evloop.SFTPProcess is None:
        print "[INFO]: sftp process return code: %s." \
            % evloop.SFTPProcess.returncode
