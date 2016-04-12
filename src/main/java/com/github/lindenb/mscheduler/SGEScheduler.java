package com.github.lindenb.mscheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.LoggerFactory;

import com.github.lindenb.mscheduler.io.IoUtils;
import com.github.lindenb.mscheduler.io.StreamBoozer;

public class SGEScheduler extends MScheduler {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SGEScheduler.class);

private SGEScheduler() {
	
}

@Override
protected  int getJobStatus(final Task t) {
	final Pattern wsSplit = Pattern.compile("[ \t]+");
	/* first we try qstat, and then qacct */
	for(int side=0; side < 2;++side)
		{
		// qacct -j 393326
		BufferedReader in = null;
		try {
		final List<String> cmdargs= new ArrayList<>();
		cmdargs.add(side==0?"qstat":"qacct");
		cmdargs.add("-j");
		cmdargs.add(t.processId);
		final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
		procbuilder.directory(getWorkingDirectory());
		final Process proc = procbuilder.start();
		StreamBoozer sb = new StreamBoozer(proc.getErrorStream(),System.err,"["+(side==0?"qstat":"qacct")+"]");
		sb.start();
		in =new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line;
		boolean found_in_qstat=false;
		while((line=in.readLine())!=null)
			{
			final String tokens[]=wsSplit.split(line);
			if(tokens.length<2) continue;
			if(side==0) {
					if(tokens[0].equals("job_number:") && tokens[1].equals(t.processId)) {
					LOG.info("job "+t.processId+" still running");
					found_in_qstat = true;
					}
				LOG.info(line);
				}
			else
				{
				
				}
			}
		in.close();
		in=null;
		
		int ret = proc.waitFor();
		if(ret!=0)
			{
			LOG.error("["+(side==0?"qstat":"qacct")+"] process failed");
			if(side==1) return -1;
			}
		if(found_in_qstat) return 0;
		
	} catch (final Exception e) {
		LOG.error("boum", e);
		return -1;	
		} finally {
			IoUtils.close(in);
		}
	}
	return -1;
}

@Override
protected void kill(final Task t) throws IOException {
	LOG.info("killing "+t);
	Runtime.getRuntime().exec("qdel "+t.processId);
	//lindenb has registered the job 393403 for deletion
}

@Override
protected int submitJob(final Task task) {
	LOG.info("Submitting "+task);
	File scriptFile=null;
	PrintWriter pw = null;
	BufferedReader in=null;
	try {
		scriptFile = File.createTempFile("tmp.", ".bash",getWorkingDirectory());
		pw = new PrintWriter(scriptFile);
		pw.println("#!/bin/bash");
		pw.println("#");
		pw.println("#$ -N "+task.getFile().getName());
		pw.println("#$ -o "+task.stdoutFile.getPath());
		pw.println("#$ -e "+task.stderrFile.getPath());
		pw.println("#$ -cwd");
		
		
		pw.println("cd '"+super.getWorkingDirectory()+"' ;");
		
		pw.println(task.shellScript);
		
		pw.flush();
		if(pw.checkError()) throw new IOException("Boum");
		pw.close();
		pw=null;
		
		makeExecutable(scriptFile);
		
		//Your job 393326 ("test.sh") has been submitted
		
		LOG.info("submitting "+task);
		final List<String> cmdargs= new ArrayList<>();
		cmdargs.add("qsub");
		cmdargs.add(task.shellScriptFile.getName());
		final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
		procbuilder.directory(task.shellScriptFile.getParentFile());
		Process proc = procbuilder.start();
		StreamBoozer sb = new StreamBoozer(proc.getErrorStream(),System.err,"[qsub]");
		sb.start();
		in =new BufferedReader(new InputStreamReader(proc.getInputStream()));
		final String qsub_prefix="Your job ";
		String line;
		while((line=in.readLine())!=null)
			{
			if(!line.startsWith(qsub_prefix))
				{
				LOG.error("expected "+line+" to start with "+qsub_prefix);
				return -1;
				}
			final int par = line.indexOf("(");
			if(par<1)
				{
				LOG.error("cannot find parenthesis in "+line);
				return -1;
				}
			
			final long sgejobid = Long.parseLong(line.substring(qsub_prefix.length(),par).trim());
			if( sgejobid <= 0L )
				{
				LOG.error("Bad job id in  "+line);
				return -1;
				}
			task.processId=String.valueOf(sgejobid);
			}
		in.close();
		
		int ret = proc.waitFor();
		if(ret!=0)
			{
			LOG.error("process failed");
			return -1;
			}
		
		if( task.processId == null )
			{
			LOG.error("Cannot get job id for "+task);
			return -1;
			}
		LOG.info("OK job ID =" + task.processId );
		
		task.startMilliSec = System.currentTimeMillis();
		task.shellScriptFile = scriptFile;
		task.targetStatus = TaskStatus.RUNNING;
		return 0;
	} catch (final Exception e) {
		LOG.error("boum", e);
		return -1;	
	} finally {
		IoUtils.close(pw);
	}
	
	}


public static void main(String[] args) {
	new SGEScheduler().instanceMainWithExit(args);
	}
}
