/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.junit;

import com.intropro.prairie.unit.common.DependencyResolver;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * Created by presidentio on 04.09.15.
 */
public class BigDataTestRunner extends BlockJUnit4ClassRunner {

    private Class<?> clazz;

    private DependencyResolver dependencyResolver;

    public BigDataTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
        this.clazz = klass;
        dependencyResolver = new DependencyResolver();
    }

    @Override
    protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
        return new BeforeStatement(super.withBefores(method, target, statement), target);
    }

    @Override
    protected Statement withAfters(FrameworkMethod method, Object target, Statement statement) {
        return new AfterStatement(super.withAfters(method, target, statement), target);
    }

    @Override
    protected Statement withBeforeClasses(Statement statement) {
        return new BeforeClassStatement(super.withBeforeClasses(statement), clazz);
    }

    @Override
    protected Statement withAfterClasses(Statement statement) {
        return new AfterClassStatement(super.withAfterClasses(statement), clazz);
    }

    private class BeforeStatement extends Statement {

        private Statement statement;

        private Object target;

        public BeforeStatement(Statement statement, Object target) {
            this.statement = statement;
            this.target = target;
        }

        @Override
        public void evaluate() throws Throwable {
            dependencyResolver.resolve(target);
            statement.evaluate();
        }

    }

    private class AfterStatement extends Statement {

        private Statement statement;

        private Object target;

        public AfterStatement(Statement statement, Object target) {
            this.statement = statement;
            this.target = target;
        }

        @Override
        public void evaluate() throws Throwable {
            statement.evaluate();
            dependencyResolver.destroy(target);
        }

    }

    private class BeforeClassStatement extends Statement {

        private Statement statement;

        private Class clazz;

        public BeforeClassStatement(Statement statement, Class clazz) {
            this.statement = statement;
            this.clazz = clazz;
        }

        @Override
        public void evaluate() throws Throwable {
            dependencyResolver.resolveStatic(clazz);
            statement.evaluate();
        }
    }

    private class AfterClassStatement extends Statement {

        private Statement statement;

        private Class clazz;

        public AfterClassStatement(Statement statement, Class clazz) {
            this.statement = statement;
            this.clazz = clazz;
        }

        @Override
        public void evaluate() throws Throwable {
            statement.evaluate();
            dependencyResolver.destroyStatic(clazz);
        }
    }
}
