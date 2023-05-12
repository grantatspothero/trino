/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.schemadiscovery;

public class DummyClassToPleaseMaven
{
    // This class is here so please maven. Starting with maven-install-plugin 3.1.1, when no module has no non-test code
    // maven complains
    //     The packaging plugin for this project did not assign a main file to the project but it has attachments. Change packaging to 'pom'
    // However, changing packaging to pom disables test execution.
}
