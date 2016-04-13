package com.github.lindenb.mscheduler;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.LoggerFactory;

import com.github.lindenb.j4make.Target;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class Task {
	@SuppressWarnings("unused")
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Task.class);
	/** script file */
	public String shellScript="";
	/** target name as defined in the Makefile */
	private String targetName;
	/** base Makefile */
	File baseDir;
	/** all prerequistites */
	private Set<String> _prerequisites= new LinkedHashSet<>();
	/** curent status */
	public TaskStatus targetStatus=TaskStatus.TOBEDONE;
	/** make nodeID */
	public long nodeId=-1L;
	/** file where the script was executed */
	public File shellScriptFile=null;
	/** job start */
	public long startMilliSec=-1L;
	/** job end */
	public long endMilliSec=-1L;
	/** error file */
	public File stdoutFile = null;
	/** stderr file */
	public File stderrFile = null;
	/** process ID */
	public String processId = null;
	
	
	static class Binding extends TupleBinding<Task>
		{
		private File readFile(final TupleInput in)
			{
			String path= in.readString();
			return path.isEmpty()?null:new File(path);
			}
		
		private void writeFile(final File file,final TupleOutput out)
			{
			out.writeString(
					file==null?
					"":
					file.getPath()
					);
			}
		
		@Override
		public Task entryToObject(final TupleInput in) {
			String s = in.readString();
			final Task t = new Task(s);
			
			t.shellScript = in.readString();
			int n = in.readInt();
			for(int i=0;i< n;++i)
				{
				t._prerequisites.add(in.readString());
				}
			
			t.targetStatus = TaskStatus.valueOf(in.readString());
			t.nodeId = in.readLong();
			
			t.shellScriptFile = readFile(in);
			t.startMilliSec = in.readLong();
			t.endMilliSec = in.readLong();
			t.baseDir = readFile(in);
			t.stdoutFile = readFile(in);
			t.stderrFile = readFile(in);
			
			t.processId = in.readString();
			if( t.processId.isEmpty()) t.processId=null;
			return t;
			}
		@Override
		public void objectToEntry(final Task t, TupleOutput out)
			{
			out.writeString(t.targetName);
			out.writeString(t.shellScript);
			out.writeInt(t._prerequisites.size());
			for(final String s: t._prerequisites)
				{
				out.writeString(s);
				}
			out.writeString(t.targetStatus.name());
			out.writeLong(t.nodeId);
			this.writeFile(t.shellScriptFile,out);
			out.writeLong(t.startMilliSec);
			out.writeLong(t.endMilliSec);
			this.writeFile(t.baseDir,out);
			this.writeFile(t.stdoutFile,out);
			this.writeFile(t.stderrFile,out);
			out.writeString(t.processId==null?"":t.processId);
			}
		}
	
	public Task(final Target t)
		{
		this(t.getName());
		this.nodeId = t.getNodeId();
		
		 final StringBuilder sbl = new StringBuilder();
    	 for(final String line: t.getShellLines()) {
    		 sbl.append(line).append("\n");
    	 }
    	 this.shellScript = sbl.toString();
    	 for(final Target c:t.getPrerequisites()) {
    		 this._prerequisites.add(c.getName());
    	 }

		}
	
	public Task(final String targetName)
		{
		this.targetName = targetName;
		}
	
	public Set<String> getPrerequisites() {
		return _prerequisites;
	}
	
	
	public void setTargetStatus(TaskStatus targetStatus) {
		this.targetStatus = targetStatus;
		}
	
	public TaskStatus getTargetStatus() {
		return this.targetStatus;
	}
	
	@Override
	public int hashCode() {
		return targetName.hashCode();
		}
	
	public String getName()
		{
		return this.targetName;
		}
	
	public File getFile()
		{
		File f=new File(getName());
		if(f.isAbsolute()) return f;
		return new File(this.baseDir,getName());
		}
	
	public boolean exists()
		{
		return getFile().exists();	
		}
	
	public long lastModified()
		{
		return getFile().lastModified();
		}
	
	
	public String md5()
		{
		return DigestUtils.md5Hex(this.targetName);
		}

	public String duration()
		{
		if(startMilliSec<0L) return "*";
		long end = (this.endMilliSec==-1?System.currentTimeMillis():endMilliSec);
		
		final long millis = end - startMilliSec;
		return String.format(
				  "%dm-%ds", 
				  TimeUnit.MILLISECONDS.toMinutes(millis),
				  TimeUnit.MILLISECONDS.toSeconds(millis) - 
				  TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
			  );
	
		}

	
	@Override
	public boolean equals(Object obj) {
		if(obj == this) return true;
		if(obj == null || !(obj instanceof Task)) return false;
		final Task other = Task.class.cast(obj);
		return getName().equals(other.getName());
		}
	
	@Override
	public String toString() {
		return "\""+getName()+"\"\tstatus:"+this.targetStatus+
				(processId==null?"":"\tprocId:"+processId)+
				(shellScriptFile==null?"":"\tshell: "+shellScriptFile)+
				(startMilliSec<0?"":"\t:duration:"+ duration())
				;
		}
}
