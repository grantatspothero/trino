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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.Session;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.IsolatedClass;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;

public interface GroupByHash
{
    NonEvictableLoadingCache<Type, Class<? extends GroupByHash>> specializedGroupByHashClasses = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                .maximumSize(256),
            CacheLoader.from(type -> isolateGroupByHashClass()));

    static Class<? extends GroupByHash> isolateGroupByHashClass()
    {
        return IsolatedClass.isolateClass(
                    new DynamicClassLoader(GroupByHash.class.getClassLoader()),
                    GroupByHash.class,
                    BigintGroupByHash.class,
                    BigintGroupByHash.AddPageWork.class,
                    BigintGroupByHash.AddDictionaryPageWork.class,
                    BigintGroupByHash.AddRunLengthEncodedPageWork.class,
                    BigintGroupByHash.GetGroupIdsWork.class,
                    BigintGroupByHash.GetDictionaryGroupIdsWork.class,
                    BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class,
                    BigintGroupByHash.DictionaryLookBack.class,
                    BigintGroupByHash.ValuesArray.class,
                    BigintGroupByHash.LongValuesArray.class,
                    BigintGroupByHash.IntegerValuesArray.class,
                    BigintGroupByHash.ShortValuesArray.class,
                    BigintGroupByHash.ByteValuesArray.class);
    }

    static GroupByHash createGroupByHash(
            Session session,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory)
    {
        return createGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, isDictionaryAggregationEnabled(session), joinCompiler, blockTypeOperators, updateMemory);
    }

    static GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory)
    {
        try {
            if (hashTypes.size() == 1 && BigintGroupByHash.isSupportedType(hashTypes.get(0)) && hashChannels.length == 1) {
                Type hashType = getOnlyElement(hashTypes);
                Constructor<? extends GroupByHash> constructor = specializedGroupByHashClasses.getUnchecked(hashType).getConstructor(int.class, boolean.class, int.class, UpdateMemory.class, Type.class);
                return constructor.newInstance(hashChannels[0], inputHashChannel.isPresent(), expectedSize, updateMemory, hashType);
            }
            return new MultiChannelGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, blockTypeOperators, updateMemory);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    long getEstimatedSize();

    List<Type> getTypes();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder);

    Work<?> addPage(Page page);

    /**
     * The order of new group ids need to be the same as the order of incoming rows,
     * i.e. new group ids should be assigned in rows iteration order
     * Example:
     * rows:      A B C B D A E
     * group ids: 1 2 3 2 4 1 5
     */
    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page, int[] hashChannels);

    default boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        return contains(position, page, hashChannels);
    }

    long getRawHash(int groupyId);

    @VisibleForTesting
    int getCapacity();
}
