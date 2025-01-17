/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ServiceThreadTest {

    @Test
    public void testShutdown() {
        shutdown(false, false);
        shutdown(false, true);
        shutdown(true, false);
        shutdown(true, true);
    }

    @Test
    public void testStop() {
        stop(true);
        stop(false);
    }

    @Test
    public void testMakeStop() {
        ServiceThread testServiceThread = startTestServiceThread();
        testServiceThread.makeStop();
        assertTrue(testServiceThread.isStopped());
    }

    @Test
    public void testWakeup() {
        ServiceThread testServiceThread = startTestServiceThread();
        testServiceThread.wakeup();
        assertTrue(testServiceThread.hasNotified.get());
        assertEquals(0, testServiceThread.waitPoint.getCount());
    }

    @Test
    public void testWaitForRunning() {
        ServiceThread testServiceThread = startTestServiceThread();
        // test waitForRunning 等一秒执行.
        testServiceThread.waitForRunning(1000);
        //true.
        assertFalse(testServiceThread.hasNotified.get());
        //
        assertEquals(1, testServiceThread.waitPoint.getCount());
        // test wake up
        testServiceThread.wakeup();
        assertTrue(testServiceThread.hasNotified.get());
        assertEquals(0, testServiceThread.waitPoint.getCount());
        // repeat waitForRunning
        testServiceThread.waitForRunning(1000);
        assertFalse(testServiceThread.hasNotified.get());
        assertEquals(0, testServiceThread.waitPoint.getCount());
        // repeat waitForRunning again
        testServiceThread.waitForRunning(1000);
        assertFalse(testServiceThread.hasNotified.get());
        assertEquals(1, testServiceThread.waitPoint.getCount());
    }

    private ServiceThread startTestServiceThread() {
        return startTestServiceThread(false);
    }

    private ServiceThread startTestServiceThread(boolean daemon) {
        ServiceThread testServiceThread = new ServiceThread() {
            @Override
            public void run() {
                doNothing();
            }

            private void doNothing() {
            }

            @Override
            public String getServiceName() {
                return "TestServiceThread";
            }
        };
        testServiceThread.setDaemon(daemon);
        // test start
        testServiceThread.start();
        assertFalse(testServiceThread.isStopped());
        return testServiceThread;
    }

    public void shutdown(boolean daemon, boolean interrupt) {
        ServiceThread testServiceThread = startTestServiceThread(daemon);
        shutdown0(interrupt, testServiceThread);
        // repeat
        shutdown0(interrupt, testServiceThread);
    }

    private void shutdown0(boolean interrupt, ServiceThread testServiceThread) {
        if (interrupt) {
            testServiceThread.shutdown(true);
        } else {
            testServiceThread.shutdown();
        }
        assertTrue(testServiceThread.isStopped());
        assertTrue(testServiceThread.hasNotified.get());
        assertEquals(0, testServiceThread.waitPoint.getCount());
    }

    public void stop(boolean interrupt) {
        ServiceThread testServiceThread = startTestServiceThread();
        stop0(interrupt, testServiceThread);
        // repeat
        stop0(interrupt, testServiceThread);
    }

    private void stop0(boolean interrupt, ServiceThread testServiceThread) {
        if (interrupt) {
            testServiceThread.stop(true);
        } else {
            testServiceThread.stop();
        }
        assertTrue(testServiceThread.isStopped());
        assertTrue(testServiceThread.hasNotified.get());
        assertEquals(0, testServiceThread.waitPoint.getCount());
    }

}
