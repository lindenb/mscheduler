/*
The MIT License (MIT)

Copyright (c) 2016 Pierre Lindenbaum

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/
package com.github.lindenb.mscheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.LoggerFactory;

import com.github.lindenb.j4make.Graph;
import com.github.lindenb.j4make.Target;
import com.github.lindenb.mscheduler.io.IoUtils;
import com.github.lindenb.mscheduler.io.StreamBoozer;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;


public abstract class MScheduler {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MScheduler.class);
	private static final String OPTION_HELP="h";
	private static final String OPTION_WORKDIR="d";
	private static final String OPTION_MAKEFILEIN="m";
	private static final String OPTION_RESETFAILURE="r";
	private static final String OPTION_N_JOBS="j";
	private Options options = new Options();
	private CommandLine cmdLine = null;
	private Environment environment = null;
	private Database targetsDatabase = null;
	private Database metaDatabase = null;
	private File workingDirectory = null;

	
protected MScheduler() {
	this.options.addOption(Option.builder(OPTION_HELP).hasArg(false).longOpt("help").hasArg(false).desc("print help").build());
	this.options.addOption(Option.builder(OPTION_WORKDIR).
			hasArg(true).
			longOpt("workdir").
			argName("DIR").
			desc("working directory").
			build());
	
}

private int parseWorkingDirectory() {
	if(!cmdLine.hasOption(OPTION_WORKDIR)) {
		LOG.error("option -"+OPTION_WORKDIR+" undefined");
		return -1;
	}
	this.workingDirectory = new File(cmdLine.getOptionValue(OPTION_WORKDIR));
	if(!this.workingDirectory.exists()) {
		LOG.error("working directory doesn't exists: "+this.workingDirectory);
		return -1;
	}
	if(!this.workingDirectory.isDirectory()) {
		LOG.error("working directory is not a directory: "+this.workingDirectory);
		return -1;
	}
	
	if(!this.workingDirectory.isAbsolute()) {
		LOG.error("working directory must be absolute: "+this.workingDirectory);
		return -1;
	}
	return 0;
}

protected void makeExecutable(final File file) throws IOException {
	Runtime.getRuntime().exec("chmod +ux "+file.getPath());
}

protected File getWorkingDirectory(){
	if(this.workingDirectory==null) throw new IllegalStateException();
	return this.workingDirectory;
}

private File getStopFile() {
	return new File(this.workingDirectory,"STOP");
}

private void createLockFile() throws IOException {
	final File lockFile = getStopFile();
	lockFile.deleteOnExit();
	final FileOutputStream fos=new FileOutputStream(lockFile);
	fos.write('\n');fos.flush();fos.close();
}

/** open bdb env */
private int openEnvironement(Transaction txn,boolean allowCreate,boolean readOnly) throws IOException {
	try {
		if(this.workingDirectory==null) {
			LOG.error("working directory undefined");
			return -1;
		}
		if(!this.workingDirectory.isDirectory() ) {
			LOG.error("working directory doesn't exist. "+this.workingDirectory);
			return -1;
		}
		if(!this.workingDirectory.isDirectory()) {
			LOG.error("working directory is not a directory " + this.workingDirectory.getPath());
			return -1;
		}
		
		if(getStopFile().exists()) {
			LOG.warn("Stop file was detected "+getStopFile());
			return -1;
		}
		
		LOG.info("opening bdb env in "+this.workingDirectory+" create:"+allowCreate+" readonly:"+readOnly);
		final EnvironmentConfig envCfg = new EnvironmentConfig();
		envCfg.setAllowCreate(allowCreate);
		envCfg.setReadOnly(readOnly);
		envCfg.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,"500000000");
		envCfg.setTransactional(false);
		this.environment = new Environment(this.workingDirectory, envCfg);
		LOG.info("opening db 'targets'");
		DatabaseConfig cfg = new DatabaseConfig();
		cfg.setAllowCreate(allowCreate);
		cfg.setReadOnly(readOnly);
		this.targetsDatabase = this.environment.openDatabase(txn, "targets",cfg);
		
		cfg = new DatabaseConfig();
		cfg.setAllowCreate(allowCreate);
		cfg.setReadOnly(readOnly);
		this.metaDatabase = this.environment.openDatabase(txn, "metaDatabase",cfg);	
		return 0;
		}
	catch(final Exception err) {
		LOG.error("cannot open db",err);
		return -1;
		}
	}

/** close BDB env */
private void close() {
	if(this.metaDatabase!=null) this.metaDatabase.close();
	this.metaDatabase=null;
	
	if(this.targetsDatabase!=null){
		this.targetsDatabase.close();
	}
	this.targetsDatabase=null;
	
	
	if(this.environment!=null)
		{
		this.environment.sync();
		if(!this.environment.getConfig().getReadOnly()){
		try { this.environment.cleanLog(); } catch(final Exception err2) {err2.printStackTrace();}
		}
		this.environment.close();
		}
	this.environment = null;
	}



private int printHelp(final String helpName) {
	final HelpFormatter fmt =new  HelpFormatter();
	final PrintWriter pw = new PrintWriter(System.out);
	pw.println(helpName);
	fmt.printOptions(pw, 80, this.options, 5, 5);
	pw.println();
	pw.flush();
	pw.close();
	return 0;
	}


private int build(final String argv[]) {
	Transaction txn = null;
	BufferedReader in = null;
	try {
		this.options.addOption(Option.builder(OPTION_MAKEFILEIN).
				hasArg(true).
				longOpt("makefile").
				argName("DIR").
				desc("debug Makefile").
				build());
		
		final CommandLineParser parser = new DefaultParser();
		this.cmdLine = parser.parse(this.options, argv);
		final List<String> args= this.cmdLine.getArgList();
		
		if(cmdLine.hasOption(OPTION_HELP)) {
			return printHelp("build");
		}
		
		if(parseWorkingDirectory()!=0) return -1;
		
		
		if(!cmdLine.hasOption(OPTION_MAKEFILEIN)) {
			LOG.error("option -"+OPTION_MAKEFILEIN+" undefined");
			return -1;
		}
		final File makefileIn =new File(cmdLine.getOptionValue(OPTION_MAKEFILEIN));
		if( !makefileIn.exists()) {
			System.err.println("Option -"+OPTION_MAKEFILEIN+" file doesn't exists: "+makefileIn);
			return -1;
		}
		
		if( !makefileIn.isFile()) {
			System.err.println("Option -"+OPTION_MAKEFILEIN+" this is not a file : "+makefileIn);
			return -1;
		}
		if( !makefileIn.isAbsolute() || makefileIn.getParentFile()==null) {
			System.err.println("Option -"+OPTION_MAKEFILEIN+" path is not absolute : "+makefileIn);
			return -1;
		}
		
		
		if(openEnvironement(txn, true,false)!=0) {
			return -1;
		}
         
     	final List<String> cmdargs= new ArrayList<>();
		cmdargs.add("make");
		cmdargs.add("-ndr");
		cmdargs.add("-C");
		cmdargs.add(makefileIn.getParentFile().getPath());
		cmdargs.add("-f");
		cmdargs.add(makefileIn.getName());
		for(final String arg: args) cmdargs.add(arg);
		
		LOG.info("invoking make :"+String.join(" ", cmdargs));
		final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
		procbuilder.directory(makefileIn.getParentFile());

		final Process proc = procbuilder.start();
		final StreamBoozer sb = new StreamBoozer(proc.getErrorStream(),System.err,"[make]");
		sb.start();
	
		LOG.info("Reading graph");
	    in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	    final Graph graph = Graph.parse(in);
	    IoUtils.close(in);in=null;
	    final Task.Binding taskBinding = new Task.Binding();
         int nTargets=0;
         LOG.info("inserting targets");
         final DatabaseEntry key = new DatabaseEntry();
         final DatabaseEntry data = new DatabaseEntry();
         for(final Target t: graph.getTargets()) {
        	 if(nTargets++%100==0) LOG.info("inserting "+t.getName()+" "+nTargets);
        	 final Task task = new Task(t);       
        	 
        	 //skip those targets, eg. "Makefile"
        	 if(task.shellScript.trim().isEmpty() && task.getPrerequisites().isEmpty()) {
        		 task.targetStatus = TaskStatus.COMPLETED;
        	 }
        	 
        	 StringBinding.stringToEntry(t.getName(), key);
        	 taskBinding.objectToEntry(task, data);
        	 if( this.targetsDatabase.put(txn, key, data) != OperationStatus.SUCCESS) {
        		 LOG.error("Cannot insert "+task);
        		 return -1;
        	 }
         }
         
         LOG.info("inserting metadata");
		return 0;
	} catch(Exception err) {
		LOG.error("Boum", err);
		return -1;
	} finally {
		close();
	}
}


protected abstract void kill(Task t)  throws IOException;


private int kill(final String argv[]) {
Cursor c = null;
Transaction txn = null;
try {
	final CommandLineParser parser = new DefaultParser();
	this.cmdLine = parser.parse(this.options, argv);
	if(!this.cmdLine.getArgList().isEmpty()) {
		LOG.error("Illegal number of arguments");
		return -1;
	}
	if(this.parseWorkingDirectory()!=0) return -1;
	if(openEnvironement(txn, false,false)!=0) return -1;
	
	
	c = this.targetsDatabase.openCursor(txn,null);
	final DatabaseEntry key=new DatabaseEntry();
	final DatabaseEntry data=new DatabaseEntry();
	final Task.Binding taskBinding = new Task.Binding();
	while(c.getNext(key, data, LockMode.DEFAULT)==OperationStatus.SUCCESS)
		{
		final Task t=taskBinding.entryToObject(data);
		if(t.getName().contains("<")) continue;//<ROOT>
		if( t.targetStatus != TaskStatus.RUNNING) continue;
		LOG.warn("killing "+t);
		kill(t);
		t.targetStatus= TaskStatus.ERROR;
		taskBinding.objectToEntry(t, data);
		if(c.putCurrent(data)!=OperationStatus.SUCCESS)
			{
			LOG.error("Cannot update "+t.getName());
			return -1;
			}
		}
	return 0;
} catch(final Exception err) {
	LOG.error("Boum", err);
	return -1;
} finally {
	IoUtils.close(c);
	close();
}
}

protected abstract int submitJob(final Task task);

private int list(final String argv[]) {
Cursor c = null;
final Transaction txn = null;
try {
	final CommandLineParser parser = new DefaultParser();
	this.cmdLine = parser.parse(this.options, argv);
	
	if(this.cmdLine.hasOption(OPTION_HELP)) {
		printHelp("Build scheduler from Makefile");
		return 0;
	}
	
	if(!this.cmdLine.getArgList().isEmpty()) {
		LOG.error("Illegal number of arguments");
		return -1;
	}
	if(this.parseWorkingDirectory()!=0) return -1;
	openEnvironement(txn, false,true);
	
	c = this.targetsDatabase.openCursor(txn,null);
	final DatabaseEntry key=new DatabaseEntry();
	final DatabaseEntry data=new DatabaseEntry();
	final Task.Binding taskBinding = new Task.Binding();
	while(c.getNext(key, data, LockMode.DEFAULT)==OperationStatus.SUCCESS)
		{
		final Task t = taskBinding.entryToObject(data);
		if(t.getName().contains("<")) continue;//<ROOT>
		
		System.out.print(t.getName());
		System.out.print('\t');
		System.out.print(t.processId==null?"*":t.processId);
		System.out.print('\t');
		System.out.print(t.targetStatus);
		System.out.print('\t');
		System.out.print(t.md5());
		System.out.print('\t');
		System.out.print(t.duration());
		System.out.print('\t');
		System.out.print(t.shellScriptFile==null?"*":t.shellScriptFile);
		
		//System.out.print('\t');
		///System.out.print(graph.mustRemake(t));
		
		System.out.print('\t');
		System.out.print(String.join(" ", t.getPrerequisites()));
		System.out.println();
		}
	return 0;
} catch(final Exception err) {
	LOG.error("Boum", err);
	return -1;
} finally {
	IoUtils.close(c);
	close();
}
}

protected abstract int updateJobStatus(final Task t);


private int runstep(final String argv[]) {
	final Transaction txn=null;
	Cursor c = null;
	int max_jobs = 1;
	try {
		
		this.options.addOption(Option.builder(OPTION_RESETFAILURE).
				hasArg(false).
				longOpt("reset").
				desc("reset failure : ERROR -> UNDEFINED").
				build()
				);	
		this.options.addOption(Option.builder(OPTION_N_JOBS).
				hasArg(true).
				longOpt("jobs").
				desc("specify number of parallel jobs").
				build()
				);	
		
		final CommandLineParser parser = new DefaultParser();
		this.cmdLine = parser.parse(this.options, argv);		
		if(!this.cmdLine.getArgList().isEmpty()) {
			LOG.error("Illegal number of arguments");
			return -1;
		}
		
		if(this.cmdLine.hasOption(OPTION_N_JOBS)) {
			max_jobs = Integer.parseInt(this.cmdLine.getOptionValue(OPTION_N_JOBS));
		}
		
		
		final boolean resetfailure=this.cmdLine.hasOption(OPTION_RESETFAILURE);
		
		if(this.parseWorkingDirectory()!=0) return -1;
		if(openEnvironement(txn, false,false)!=0) return -1;

		Task last_failed_job = null;
		
		/* loop over jobs, check there is no previous FAILED status */
		c = this.targetsDatabase.openCursor(txn, null);
		final Task.Binding taskBinding = new Task.Binding();
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry data=new DatabaseEntry();
		while(c.getNext(key, data, LockMode.DEFAULT)==OperationStatus.SUCCESS) {
			final Task jobInfo = taskBinding.entryToObject(data);
			if(jobInfo.getName().contains("<")) continue;//<ROOT>
			switch(jobInfo.targetStatus)
				{
				case TOBEDONE: break;
				case COMPLETED: break;
				case ERROR:
					last_failed_job = jobInfo;
					LOG.error("job "+jobInfo+" failed.. Use -reset to reset values");
					if(resetfailure)
						{
						LOG.warn("Reset status of "+jobInfo);
						jobInfo.targetStatus = TaskStatus.TOBEDONE;
						taskBinding.objectToEntry(jobInfo, data);
						if(c.putCurrent(data)!=OperationStatus.SUCCESS) {
							LOG.error("Cannot reset status of "+jobInfo);
							return -1;
							}
						}
					continue;
				
				/* last time we checked, the job was running */
				case RUNNING:
					{
					LOG.info("checking if job is running "+jobInfo);
					if(updateJobStatus(jobInfo)!=0) {
						return -1;
						}
			        /* check new target status */
					switch(jobInfo.targetStatus)
						{
						case RUNNING:
							{
							LOG.info("still running :" + jobInfo.getName());
							max_jobs = Math.max(max_jobs-1,0); 
							// we already know it's running, don't do anything
							// jobInfo.targetStatus  =  TargetStatus.RUNNING;
							// graph.persist(jobInfo);
							break;
							}
						case ERROR: 
							{
							LOG.info("job failed:" + jobInfo.getName());
							last_failed_job = jobInfo;
							jobInfo.targetStatus = TaskStatus.ERROR;
							jobInfo.endMilliSec = System.currentTimeMillis();
							taskBinding.objectToEntry(jobInfo, data);
							if(c.putCurrent(data)!=OperationStatus.SUCCESS) {
								LOG.error("Cannot update status of "+jobInfo);
								return -1;
								}
							break;
							}
						case COMPLETED: 
							{
							LOG.info("job completed:" + jobInfo.getName());
							jobInfo.targetStatus = TaskStatus.COMPLETED;
							if( jobInfo.shellScriptFile!=null && jobInfo.shellScriptFile.exists())
								{
								LOG.warn("deleting "+jobInfo.shellScriptFile);
								jobInfo.shellScriptFile.delete();
								jobInfo.shellScriptFile=null;
								
								if(jobInfo.stdoutFile!=null && jobInfo.stdoutFile.exists())
									{
									jobInfo.stdoutFile.delete();
									jobInfo.stdoutFile=null;
									}
								if(jobInfo.stderrFile!=null && jobInfo.stderrFile.exists())
									{
									jobInfo.stderrFile.delete();
									jobInfo.stderrFile=null;
									}								
								}
							jobInfo.endMilliSec = System.currentTimeMillis();
							
							taskBinding.objectToEntry(jobInfo, data);
							if(c.putCurrent(data)!=OperationStatus.SUCCESS) {
								LOG.error("Cannot update status of "+jobInfo);
								return -1;
								}
							break;
							}
						default: throw new IllegalStateException(jobInfo+" "+jobInfo.targetStatus);
						}
					break;
					}
				default: throw new IllegalStateException(jobInfo+" "+jobInfo.targetStatus);
				}
				
			}
		c.close();c=null;
		
		if(last_failed_job!=null) {
			LOG.error("Exiting because : Job failed "+last_failed_job);
			return -1;
		}
		
		if(max_jobs<1) {
			LOG.error("Exiting because : max_jobs < 1");
			return 0;
		}
		
		final List<Task> targetsToDo = new ArrayList<>();
		c = this.targetsDatabase.openCursor(txn, null);
		key=new DatabaseEntry();
		data=new DatabaseEntry();
		while(c.getNext(key, data, LockMode.DEFAULT)==OperationStatus.SUCCESS  && targetsToDo.size()< max_jobs ) {
			final Task t = taskBinding.entryToObject(data);
			if(t.getName().contains("<")) continue;//<ROOT>
			if(t.targetStatus==TaskStatus.COMPLETED || t.targetStatus==TaskStatus.RUNNING) continue;
			if(t.targetStatus==TaskStatus.ERROR) {
				LOG.warn("Uhhh??");
				return -1;
				}
			if(t.targetStatus!=TaskStatus.TOBEDONE) {
				LOG.warn("Uhhh??");
				return -1;
				}
			boolean ok=true;
			final DatabaseEntry key2=new DatabaseEntry();
			final DatabaseEntry data2=new DatabaseEntry();
			for(final String prereqName: t.getPrerequisites()) {
				StringBinding.stringToEntry(prereqName, key2);
				if(this.targetsDatabase.get(txn, key2, data2, LockMode.DEFAULT)!=OperationStatus.SUCCESS) {
					LOG.error("Cannot get prerequsiste "+prereqName+" from "+t);
					return -1;
				}
				final Task t2 = taskBinding.entryToObject(data2);
				switch(t2.targetStatus) {
					case COMPLETED: break;
					case ERROR: ok = false; break;
					case RUNNING: ok= false; break;
					case TOBEDONE: ok=false; break;
					default: throw new IllegalStateException("err");
					}
				if(!ok) break;
			}
			
			if(!ok) continue;
			targetsToDo.add(t);
			}
		c.close();c=null;
		

		if(resetfailure)
			{
			LOG.info("exiting after reset");
			return 0;
			}
		
		/* we found one failed job, exit with failure */
		if( last_failed_job != null)
			{
			LOG.error("Found failure jobs e.g: "+last_failed_job+". exiting.");
			return -1;
			}
		
		LOG.info("submitting "+targetsToDo.size()+" job(s)");
		for(final Task task: targetsToDo)
			{
			if(task.targetStatus!=TaskStatus.TOBEDONE) {
				throw new IllegalStateException("shouldn't submit "+task);
			}
			if(submitJob(task)!=0) return -1;
			if(task.targetStatus!=TaskStatus.RUNNING) {
				throw new IllegalStateException();
			}
			LOG.info("updating "+task);
			StringBinding.stringToEntry(task.getName(), key);
			taskBinding.objectToEntry(task, data);
			if(this.targetsDatabase.put(txn, key, data)!=OperationStatus.SUCCESS) {
				LOG.error("Cannot update "+task);
				return -1;
				}			
			}
		LOG.info("exiting SUCCESS");	
		

		return 0;
	} catch(Exception err) {
		LOG.error("Boum", err);
		return -1;
	} finally {
		close();
	}
}


private int instanceMain(final String[] args) {

	if(args.length<1) {
		System.err.println("Usage: ");
		System.err.println(" kill ");
		System.err.println(" list ");
		System.err.println(" build : create new scheduler from an existing Makefile ");
		System.err.println(" run ");
		return -1;
	} else if(args[0].equals("kill")) {
		final String args2[]=new String[args.length-1];
		System.arraycopy(args, 1, args2, 0, args2.length);
		return kill(args2);
	} else if(args[0].equals("list")) {
		final String args2[]=new String[args.length-1];
		System.arraycopy(args, 1, args2, 0, args2.length);
		return list(args2);
	} else if(args[0].equals("build")) {
		final String args2[]=new String[args.length-1];
		System.arraycopy(args, 1, args2, 0, args2.length);
		return build(args2);
	} else if(args[0].equals("run")) {
		final String args2[]=new String[args.length-1];
		System.arraycopy(args, 1, args2, 0, args2.length);
		return runstep(args2);
	}
	else {
		LOG.error("Unknown command "+args[0]);
		return -1;
		}
	
	}


protected void instanceMainWithExit(String[] args) {
	System.exit(instanceMain(args));
	}


}
