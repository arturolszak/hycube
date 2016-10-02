package net.hycube.maintenance;

import net.hycube.backgroundprocessing.BackgroundProcessEntryPoint;

public interface HyCubeRecoveryBackgroundProcessEntryPoint extends BackgroundProcessEntryPoint {

	public void setRecoveryPlan(HyCubeRecoveryType[] recoveryPlan, int scheduleInterval);
	public void setRecoveryPlan(HyCubeRecoveryType[] recoveryPlan);
	public void setRecoveryScheduleInterval(int scheduleInterval);

	
}
