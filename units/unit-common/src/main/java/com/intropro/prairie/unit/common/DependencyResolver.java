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
package com.intropro.prairie.unit.common;

import com.intropro.prairie.unit.common.annotation.PrairieUnit;
import com.intropro.prairie.unit.common.exception.PrairieException;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by presidentio on 04.09.15.
 */
public class DependencyResolver {

    private Map<Class, Unit> units = new HashMap<Class, Unit>();

    private Map<Class, List<Object>> dependencies = new HashMap<Class, List<Object>>();

    public void resolve(Object object) throws PrairieException {
        resolve(object, object.getClass());
    }

    private void resolve(Object object, Class clazz) throws PrairieException {
        Class parentClass = clazz.getSuperclass();
        if(parentClass != null && !parentClass.equals(Object.class)) {
            resolve(object, parentClass);
        }
        for (Field field : clazz.getDeclaredFields()) {
            PrairieUnit prairieUnit = field.getAnnotation(PrairieUnit.class);
            if (prairieUnit != null && !Modifier.isStatic(field.getModifiers())) {
                resolveDependency(field, object);
            }
        }
    }

    public void resolveStatic(Class clazz) throws PrairieException {
        for (Field field : clazz.getDeclaredFields()) {
            PrairieUnit prairieUnit = field.getAnnotation(PrairieUnit.class);
            if (prairieUnit != null && Modifier.isStatic(field.getModifiers())) {
                resolveDependency(field, clazz);
            }
        }
    }

    private void resolveDependency(Field field, Object ref) throws PrairieException {
        Class bigDataComponentClass = field.getType();
        if (Unit.class.isAssignableFrom(bigDataComponentClass)) {
            Unit unit = getUnit(bigDataComponentClass);
            setDependency(ref, field, unit);
        } else {
            throw new PrairieException(
                    String.format("%s doesn't implements EmbeddedComponent interface",
                            bigDataComponentClass.getSimpleName()));
        }
    }

    public void destroy(Object object) throws DestroyUnitException {
        destroy(object, object.getClass());
        garbageCollector();
    }

    private void destroy(Object object, Class clazz) throws DestroyUnitException {
        for (Field field : clazz.getDeclaredFields()) {
            PrairieUnit prairieUnit = field.getAnnotation(PrairieUnit.class);
            if (prairieUnit != null && !Modifier.isStatic(field.getModifiers())) {
                List<Object> links = dependencies.get(field.getType());
                if(links != null) {
                    links.remove(object);
                }
            }
        }
        Class parentClass = clazz.getSuperclass();
        if(parentClass != null && !parentClass.equals(Object.class)) {
            destroy(object, parentClass);
        }
    }

    public void destroyStatic(Class clazz) throws DestroyUnitException {
        for (Field field : clazz.getClass().getDeclaredFields()) {
            PrairieUnit prairieUnit = field.getAnnotation(PrairieUnit.class);
            if (prairieUnit != null && Modifier.isStatic(field.getModifiers())) {
                List<Object> links = dependencies.get(field.getType());
                if(links != null) {
                    links.remove(clazz);
                }
            }
        }
        garbageCollector();
    }

    private void garbageCollector() throws DestroyUnitException {
        boolean destroyAtLeastOne = true;
        while (destroyAtLeastOne) {
            destroyAtLeastOne = false;
            List<Class> destroyedComponents = new ArrayList<>();
            for (Map.Entry<Class, Unit> classEmbeddedComponentEntry : units.entrySet()) {
                if (dependencies.get(classEmbeddedComponentEntry.getKey()).isEmpty()) {
                    for (List<Object> objects : dependencies.values()) {
                        objects.remove(classEmbeddedComponentEntry.getValue());
                    }
                    classEmbeddedComponentEntry.getValue().stop();
                    destroyedComponents.add(classEmbeddedComponentEntry.getKey());
                    destroyAtLeastOne = true;
                }
            }
            for (Class destroyedComponent : destroyedComponents) {
                units.remove(destroyedComponent);
            }
        }
    }

    private <T extends Unit> T getUnit(Class<T> clazz) throws PrairieException {
        try {
            T embeddedComponent = (T) units.get(clazz);
            if (embeddedComponent == null) {
                embeddedComponent = clazz.newInstance();
                units.put(clazz, embeddedComponent);
                dependencies.put(clazz, new ArrayList<>());
                resolve(embeddedComponent);
                embeddedComponent.start();
            }
            return embeddedComponent;
        } catch (InstantiationException | IllegalAccessException e) {
            units.remove(clazz);
            throw new PrairieException(e);
        }
    }

    private void setDependency(Object target, Field field, Object dependency) throws PrairieException {
        field.setAccessible(true);
        try {
            field.set(target, dependency);
            List<Object> dependentObjects = dependencies.get(dependency.getClass());
            if (dependentObjects == null) {
                dependentObjects = new ArrayList<>();
                dependencies.put(dependency.getClass(), dependentObjects);
            }
            dependentObjects.add(target);
        } catch (IllegalAccessException e) {
            throw new PrairieException(
                    String.format("Field %s must be public", field.getName()));
        }
    }

}
